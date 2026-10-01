from __future__ import annotations

from contextlib import contextmanager

from psycopg2.extras import Json, RealDictCursor

from app import custom_ontology_service as custom_svc
from app import ontology_publish
from app.cphr_ontology_service import load_ontology_snapshot
from app.db import get_connection

# Backs the two-step "map this dataset's fields to an ontology" flow:
# 1) pick an ontology (predefined or custom-built)
# 2) pick, per dataset field, which of that ontology's fields it corresponds to
#
# This is deliberately a *new* module rather than a patch to the existing
# POST /dataset-mapping-update (app/endpoints/dataset_details.py) -- that one
# is wired to a live frontend contract (field_name/ontology_mapping/description)
# and its own lookup bug (it resolves ontology_mapping by searching
# dataset_mapping itself, not the real ontology) is left alone here rather
# than silently changed underneath that contract. This module is additive;
# the old endpoint can be deprecated/removed once a frontend migrates to it,
# same pattern as /ontology/v3 sitting next to the older /ontology/* routes.


class DatasetNotFoundError(Exception):
    pass


class OntologyNotFoundError(Exception):
    pass


class FieldNotFoundError(Exception):
    pass


class MappingNotFoundError(Exception):
    pass


class InvalidMappingError(Exception):
    pass


@contextmanager
def _cursor():
    conn = get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                yield cur
    finally:
        conn.close()


def list_registered_ontologies() -> list[dict]:
    """Every ontology a dataset can be mapped against: the predefined,
    file-based ones (app/ontology_publish.py's registry) plus every
    user-authored one (custom_ontology_service)."""
    items = []
    for graph_key, entry in ontology_publish.get_registry().items():
        items.append({"graph_key": graph_key, "title": entry["label"], "source": "predefined", "status": None})
    for ontology in custom_svc.list_ontologies():
        items.append(
            {
                "graph_key": ontology["graph_key"],
                "title": ontology["title"],
                "source": "custom",
                "status": ontology["status"],
            }
        )
    return items


def _predefined_fields(graph_key: str) -> list[dict]:
    """The ontology's terms (its non-deprecated classes, e.g. ENVO 'season'),
    not its relations ('part of', 'has part') -- a dataset column stands for
    a term. value is the term's IRI, so a saved mapping's ontology_mapping
    matches the cml_term crosswalk the same way the curated rows do."""
    snapshot = load_ontology_snapshot(graph_key)
    return [
        {"value": cls["iri"], "label": cls["label"], "uri": cls["iri"]}
        for cls in sorted(snapshot["classes"], key=lambda c: c["label"].lower())
    ]


def _custom_fields(graph_key: str) -> list[dict]:
    return [
        {
            "value": p["name"],
            "label": p["label"],
            "class_name": p["domain_class_name"],
            "property_type": p["property_type"],
            "range": p["range_value"],
            # same namespace custom_ontology_service's TTL export publishes under
            "uri": f"http://cml.org/ontology/{graph_key}#{p['name']}",
        }
        for p in custom_svc.list_all_properties(graph_key)
    ]


def get_ontology_fields(graph_key: str) -> dict:
    """(source, fields) for the given ontology -- tries the predefined
    registry first, then custom ontologies. Raises OntologyNotFoundError if
    graph_key is unknown to both."""
    if graph_key in ontology_publish.get_registry():
        return {"graph_key": graph_key, "source": "predefined", "items": _predefined_fields(graph_key)}

    if custom_svc.get_ontology(graph_key):
        return {"graph_key": graph_key, "source": "custom", "items": _custom_fields(graph_key)}

    raise OntologyNotFoundError(graph_key)


def _dataset_exists(cur, dataset_id: int) -> bool:
    cur.execute("SELECT 1 FROM dataset_master WHERE dataset_id = %s", (dataset_id,))
    return cur.fetchone() is not None


def save_mappings(dataset_id: int, ontology_graph_key: str, mappings: list[dict]) -> dict:
    """mappings: [{field_name, ontology_field, sample_value, value_range,
    ontology_uri, metadata, label, data_type}, ...]. Each ontology_field is validated
    against the chosen ontology's real field list before insert -- the fix
    for the old endpoint's self-referential (and often silently no-op)
    lookup.

    A field the contributor couldn't find in the ontology comes with no
    ontology_field and only an ontology_uri: it's stored as-is under its
    own field name, with no ontology_graph_key since it isn't a term of the
    chosen ontology."""
    field_index = {f["value"]: f for f in get_ontology_fields(ontology_graph_key)["items"]}

    with _cursor() as cur:
        if not _dataset_exists(cur, dataset_id):
            raise DatasetNotFoundError(dataset_id)

        inserted = []
        for mapping in mappings:
            field_name = mapping["field_name"]
            ontology_field = mapping.get("ontology_field")
            ontology_uri = mapping.get("ontology_uri")

            if ontology_field:
                field_info = field_index.get(ontology_field)
                if not field_info:
                    raise FieldNotFoundError(
                        f"{ontology_field!r} is not a field of ontology {ontology_graph_key!r}"
                    )
                if "property_type" in field_info:  # custom ontology property
                    data_type = field_info["range"] if field_info["property_type"] == "datatype" else "object"
                else:  # predefined ontology term: the column keeps its own type
                    data_type = mapping.get("data_type")
                row = (ontology_field, field_info["label"], data_type, ontology_graph_key,
                       ontology_uri or field_info.get("uri"))
            elif ontology_uri:
                row = (field_name, mapping.get("label") or field_name, mapping.get("data_type"), None,
                       ontology_uri)
            else:
                raise InvalidMappingError(f"{field_name!r} needs either an ontology_field or an ontology_uri")

            cur.execute(
                """
                INSERT INTO dataset_mapping
                    (dataset_id, field_name, ontology_mapping, ontology_mapping_to_display, data_type,
                     ontology_graph_key, ontology_uri, sample_value, value_range, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    dataset_id, field_name, *row, mapping.get("sample_value"), mapping.get("value_range"),
                    Json(mapping["metadata"]) if mapping.get("metadata") is not None else None,
                ),
            )
            inserted.append(dict(cur.fetchone()))

    return {"dataset_id": dataset_id, "ontology_graph_key": ontology_graph_key, "count": len(inserted), "items": inserted}


def list_mappings(dataset_id: int) -> list[dict]:
    with _cursor() as cur:
        if not _dataset_exists(cur, dataset_id):
            raise DatasetNotFoundError(dataset_id)
        cur.execute(
            "SELECT * FROM dataset_mapping WHERE dataset_id = %s ORDER BY dataset_mapping_id", (dataset_id,)
        )
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def delete_mapping(dataset_id: int, dataset_mapping_id: int) -> bool:
    with _cursor() as cur:
        cur.execute(
            "DELETE FROM dataset_mapping WHERE dataset_id = %s AND dataset_mapping_id = %s",
            (dataset_id, dataset_mapping_id),
        )
        deleted = cur.rowcount > 0
    if not deleted:
        raise MappingNotFoundError(dataset_mapping_id)
    return True
