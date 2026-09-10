from __future__ import annotations

import re
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor

from app.db import get_connection

# ---------------------------------------------------------------------------
# Errors -- endpoints/cphr_ontology_builder.py maps these to HTTP status codes
# (404 / 409 / 400) instead of every service function taking an HTTPException
# dependency.
# ---------------------------------------------------------------------------


class OntologyNotFoundError(Exception):
    pass


class ClassNotFoundError(Exception):
    pass


class PropertyNotFoundError(Exception):
    pass


class MappingNotFoundError(Exception):
    pass


class VersionNotFoundError(Exception):
    pass


class ConflictError(Exception):
    pass


class ValidationError(Exception):
    pass


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Same shape as ontology_publish._GRAPH_KEY_PATTERN -- graph_key doubles as a
# file name and a Fuseki graph URI segment at publish time.
_GRAPH_KEY_RE = re.compile(r"^[a-zA-Z0-9_-]+$")

# Local Turtle name: must be usable directly after "cphr:" with no escaping.
_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_CURIE_RE = re.compile(r"^[A-Za-z][\w-]*:[^\s]+$")
_URI_RE = re.compile(r"^<[^\s<>]+>$")

XSD_TYPES = {
    "string", "integer", "boolean", "double", "decimal",
    "date", "dateTime", "anyURI", "float",
}

RELATION_TYPES = {
    "owl:equivalentClass", "skos:exactMatch", "skos:closeMatch", "skos:relatedMatch",
}

TARGET_KINDS = {"internal", "external_ontology", "external_uri"}

_ONTOLOGY_UPDATABLE_FIELDS = {
    "title", "acronym", "visibility", "status", "description",
    "categories", "bibliographic_refs", "contact",
}
_CLASS_UPDATABLE_FIELDS = {"label", "definition", "synonyms", "parent_name"}


@contextmanager
def _cursor():
    conn = get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                yield cur
    finally:
        conn.close()


def _row(row):
    return dict(row) if row is not None else None


# ---------------------------------------------------------------------------
# Ontology CRUD
# ---------------------------------------------------------------------------


def create_ontology(
    graph_key: str,
    title: str,
    acronym: str,
    visibility: str = "Public",
    status: str = "Staging",
    description: str | None = None,
    categories: list[str] | None = None,
    bibliographic_refs: list[str] | None = None,
    contact: str | None = None,
) -> dict:
    if not _GRAPH_KEY_RE.match(graph_key):
        raise ValidationError(
            f"graph_key {graph_key!r} must match ^[a-zA-Z0-9_-]+$ (used as a file name and Fuseki graph segment)"
        )
    with _cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO custom_ontology
                    (graph_key, title, acronym, visibility, status, description, categories, bibliographic_refs, contact)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    graph_key, title, acronym, visibility, status, description,
                    categories or [], bibliographic_refs or [], contact,
                ),
            )
            row = cur.fetchone()
        except psycopg2.errors.UniqueViolation:
            raise ConflictError(f"An ontology with graph_key {graph_key!r} already exists")
    return _row(row)


def list_ontologies() -> list[dict]:
    with _cursor() as cur:
        cur.execute(
            """
            SELECT o.*, COUNT(c.id) AS class_count
            FROM custom_ontology o
            LEFT JOIN custom_class c ON c.graph_key = o.graph_key
            GROUP BY o.graph_key
            ORDER BY o.updated_at DESC
            """
        )
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def get_ontology(graph_key: str) -> dict | None:
    with _cursor() as cur:
        cur.execute("SELECT * FROM custom_ontology WHERE graph_key = %s", (graph_key,))
        row = cur.fetchone()
    return _row(row)


def update_ontology(graph_key: str, updates: dict) -> dict | None:
    updates = {k: v for k, v in updates.items() if k in _ONTOLOGY_UPDATABLE_FIELDS}
    with _cursor() as cur:
        if not updates:
            cur.execute("SELECT * FROM custom_ontology WHERE graph_key = %s", (graph_key,))
            return _row(cur.fetchone())
        set_clause = ", ".join(f"{key} = %s" for key in updates)
        values = list(updates.values()) + [graph_key]
        cur.execute(
            f"UPDATE custom_ontology SET {set_clause}, updated_at = NOW() WHERE graph_key = %s RETURNING *",
            values,
        )
        row = cur.fetchone()
    return _row(row)


def delete_ontology(graph_key: str) -> bool:
    """Deletes the draft only (cascades to its classes/properties/mappings/
    versions). If it was ever published, the TTL file and Fuseki graph are
    left in place -- remove them separately via the existing generic
    DELETE /ontology/ttl/{graph_key} (app/endpoints/ontology_publish.py)."""
    with _cursor() as cur:
        cur.execute("DELETE FROM custom_ontology WHERE graph_key = %s", (graph_key,))
        return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Class CRUD
# ---------------------------------------------------------------------------


def _ontology_exists(cur, graph_key: str) -> bool:
    cur.execute("SELECT 1 FROM custom_ontology WHERE graph_key = %s", (graph_key,))
    return cur.fetchone() is not None


def _class_exists(cur, graph_key: str, name: str) -> bool:
    cur.execute("SELECT 1 FROM custom_class WHERE graph_key = %s AND name = %s", (graph_key, name))
    return cur.fetchone() is not None


def _validate_name(name: str, kind: str) -> None:
    if not _NAME_RE.match(name):
        raise ValidationError(f"{kind} name {name!r} must match ^[A-Za-z_][A-Za-z0-9_]*$")


def create_class(
    graph_key: str,
    name: str,
    label: str,
    definition: str | None = None,
    synonyms: list[str] | None = None,
    parent_name: str | None = None,
) -> dict:
    _validate_name(name, "Class")
    with _cursor() as cur:
        if not _ontology_exists(cur, graph_key):
            raise OntologyNotFoundError(graph_key)
        if parent_name is not None and not _class_exists(cur, graph_key, parent_name):
            raise ClassNotFoundError(f"Parent class {parent_name!r} not found in ontology {graph_key!r}")
        try:
            cur.execute(
                """
                INSERT INTO custom_class (graph_key, name, label, definition, synonyms, parent_name)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (graph_key, name, label, definition, synonyms or [], parent_name),
            )
            row = cur.fetchone()
        except psycopg2.errors.UniqueViolation:
            raise ConflictError(f"Class {name!r} already exists in ontology {graph_key!r}")
    return _row(row)


def _walk_ancestor_names(cur, graph_key: str, start_name: str):
    """Ancestor names from start_name's parent up to the root, guarding
    against an already-corrupt cycle looping forever."""
    current = start_name
    seen: set[str] = set()
    while True:
        cur.execute(
            "SELECT parent_name FROM custom_class WHERE graph_key = %s AND name = %s",
            (graph_key, current),
        )
        row = cur.fetchone()
        parent = row["parent_name"] if row else None
        if not parent or parent in seen:
            return
        seen.add(parent)
        yield parent
        current = parent


def update_class(graph_key: str, name: str, updates: dict) -> dict:
    updates = {k: v for k, v in updates.items() if k in _CLASS_UPDATABLE_FIELDS}
    with _cursor() as cur:
        if not _class_exists(cur, graph_key, name):
            raise ClassNotFoundError(name)

        new_parent = updates.get("parent_name")
        if "parent_name" in updates and new_parent is not None:
            if new_parent == name:
                raise ValidationError("A class cannot be its own parent")
            if not _class_exists(cur, graph_key, new_parent):
                raise ClassNotFoundError(f"Parent class {new_parent!r} not found in ontology {graph_key!r}")
            if name in set(_walk_ancestor_names(cur, graph_key, new_parent)):
                raise ValidationError(f"Setting parent to {new_parent!r} would create a cycle")

        if not updates:
            cur.execute("SELECT * FROM custom_class WHERE graph_key = %s AND name = %s", (graph_key, name))
            return _row(cur.fetchone())

        set_clause = ", ".join(f"{key} = %s" for key in updates)
        values = list(updates.values()) + [graph_key, name]
        cur.execute(
            f"UPDATE custom_class SET {set_clause}, updated_at = NOW() WHERE graph_key = %s AND name = %s RETURNING *",
            values,
        )
        row = cur.fetchone()
    return _row(row)


def delete_class(graph_key: str, name: str) -> bool:
    with _cursor() as cur:
        if not _class_exists(cur, graph_key, name):
            raise ClassNotFoundError(name)
        try:
            cur.execute("DELETE FROM custom_class WHERE graph_key = %s AND name = %s", (graph_key, name))
        except psycopg2.errors.ForeignKeyViolation:
            raise ConflictError(f"Class {name!r} still has subclasses -- delete those first")
    return True


def _hydrate_class(cur, graph_key: str, class_row: dict) -> dict:
    name = class_row["name"]

    cur.execute(
        """
        SELECT name, label, comment, property_type, domain_class_name, range_value, cardinality_note
        FROM custom_property WHERE graph_key = %s AND domain_class_name = %s ORDER BY name
        """,
        (graph_key, name),
    )
    properties = [dict(r) for r in cur.fetchall()]

    cur.execute(
        """
        SELECT id, relation, target_kind, to_class_name, to_ontology_graph_key, to_ref
        FROM custom_class_mapping WHERE graph_key = %s AND from_class_name = %s ORDER BY id
        """,
        (graph_key, name),
    )
    mappings = [dict(r) for r in cur.fetchall()]

    cur.execute(
        "SELECT name FROM custom_class WHERE graph_key = %s AND parent_name = %s ORDER BY name",
        (graph_key, name),
    )
    children = [r["name"] for r in cur.fetchall()]

    return {
        "name": name,
        "label": class_row["label"],
        "definition": class_row["definition"],
        "synonyms": class_row["synonyms"],
        "parent_name": class_row["parent_name"],
        "children": children,
        "has_children": bool(children),
        "datatype_properties": [p for p in properties if p["property_type"] == "datatype"],
        "object_properties": [p for p in properties if p["property_type"] == "object"],
        "mappings": mappings,
    }


def list_classes(graph_key: str, parent: str | None = None) -> dict:
    """Same no-parameter/parent-shaped contract as the predefined ontology's
    GET /ontology/v3/classes -- no parent = top-level classes, parent set =
    that class's direct children. Each item carries its own properties and
    mappings so the caller can render one tree level per call."""
    with _cursor() as cur:
        if not _ontology_exists(cur, graph_key):
            raise OntologyNotFoundError(graph_key)

        if parent:
            if not _class_exists(cur, graph_key, parent):
                raise ClassNotFoundError(parent)
            cur.execute(
                "SELECT * FROM custom_class WHERE graph_key = %s AND parent_name = %s ORDER BY name",
                (graph_key, parent),
            )
        else:
            cur.execute(
                "SELECT * FROM custom_class WHERE graph_key = %s AND parent_name IS NULL ORDER BY name",
                (graph_key,),
            )
        class_rows = cur.fetchall()
        items = [_hydrate_class(cur, graph_key, dict(c)) for c in class_rows]

    return {"parent": parent, "count": len(items), "items": items}


def list_all_properties(graph_key: str) -> list[dict]:
    """Every property in the ontology regardless of owning class -- used by
    dataset_ontology_mapping_service to populate the field-mapping dropdown."""
    with _cursor() as cur:
        if not _ontology_exists(cur, graph_key):
            raise OntologyNotFoundError(graph_key)
        cur.execute(
            """
            SELECT name, label, comment, property_type, domain_class_name, range_value, cardinality_note
            FROM custom_property WHERE graph_key = %s ORDER BY domain_class_name, name
            """,
            (graph_key,),
        )
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def get_class(graph_key: str, name: str) -> dict:
    with _cursor() as cur:
        cur.execute("SELECT * FROM custom_class WHERE graph_key = %s AND name = %s", (graph_key, name))
        row = cur.fetchone()
        if not row:
            raise ClassNotFoundError(name)
        return _hydrate_class(cur, graph_key, dict(row))


# ---------------------------------------------------------------------------
# Property CRUD
# ---------------------------------------------------------------------------


def create_property(
    graph_key: str,
    class_name: str,
    name: str,
    label: str,
    property_type: str,
    range_value: str,
    comment: str | None = None,
    cardinality_note: str | None = None,
) -> dict:
    _validate_name(name, "Property")
    if property_type not in ("datatype", "object"):
        raise ValidationError("property_type must be 'datatype' or 'object'")

    with _cursor() as cur:
        if not _class_exists(cur, graph_key, class_name):
            raise ClassNotFoundError(class_name)

        if property_type == "datatype":
            normalized_range = range_value.removeprefix("xsd:")
            if normalized_range not in XSD_TYPES:
                raise ValidationError(f"Unknown xsd type {range_value!r}. Allowed: {sorted(XSD_TYPES)}")
        else:
            normalized_range = range_value.removeprefix("cphr:")
            if not _class_exists(cur, graph_key, normalized_range):
                raise ClassNotFoundError(
                    f"Object property range {range_value!r} is not a class in ontology {graph_key!r}"
                )

        try:
            cur.execute(
                """
                INSERT INTO custom_property
                    (graph_key, name, label, comment, property_type, domain_class_name, range_value, cardinality_note)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (graph_key, name, label, comment, property_type, class_name, normalized_range, cardinality_note),
            )
            row = cur.fetchone()
        except psycopg2.errors.UniqueViolation:
            raise ConflictError(f"Property {name!r} already exists in ontology {graph_key!r}")
    return _row(row)


def delete_property(graph_key: str, name: str) -> bool:
    with _cursor() as cur:
        cur.execute("DELETE FROM custom_property WHERE graph_key = %s AND name = %s", (graph_key, name))
        deleted = cur.rowcount > 0
    if not deleted:
        raise PropertyNotFoundError(name)
    return True


# ---------------------------------------------------------------------------
# Mapping CRUD
# ---------------------------------------------------------------------------


def _known_predefined_graph_keys() -> set[str]:
    from app import ontology_publish

    return set(ontology_publish.get_registry().keys())


def create_mapping(
    graph_key: str,
    class_name: str,
    relation: str,
    target_kind: str,
    to_class_name: str | None = None,
    to_ontology_graph_key: str | None = None,
    to_ref: str | None = None,
) -> dict:
    if relation not in RELATION_TYPES:
        raise ValidationError(f"relation must be one of {sorted(RELATION_TYPES)}")
    if target_kind not in TARGET_KINDS:
        raise ValidationError(f"target_kind must be one of {sorted(TARGET_KINDS)}")

    with _cursor() as cur:
        if not _class_exists(cur, graph_key, class_name):
            raise ClassNotFoundError(class_name)

        if target_kind == "internal":
            if not to_class_name:
                raise ValidationError("to_class_name is required when target_kind='internal'")
            if not _class_exists(cur, graph_key, to_class_name):
                raise ClassNotFoundError(to_class_name)
            to_ontology_graph_key, to_ref = None, None
        elif target_kind == "external_ontology":
            if not to_ontology_graph_key or not to_ref:
                raise ValidationError(
                    "to_ontology_graph_key and to_ref are both required when target_kind='external_ontology'"
                )
            if not _ontology_exists(cur, to_ontology_graph_key) and to_ontology_graph_key not in _known_predefined_graph_keys():
                raise ValidationError(f"Unknown ontology {to_ontology_graph_key!r}")
            to_class_name = None
        else:  # external_uri
            if not to_ref or not (_CURIE_RE.match(to_ref) or _URI_RE.match(to_ref)):
                raise ValidationError("to_ref must be a CURIE ('prefix:local') or a full URI wrapped in <>")
            to_class_name, to_ontology_graph_key = None, None

        cur.execute(
            """
            INSERT INTO custom_class_mapping
                (graph_key, from_class_name, relation, target_kind, to_class_name, to_ontology_graph_key, to_ref)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (graph_key, class_name, relation, target_kind, to_class_name, to_ontology_graph_key, to_ref),
        )
        row = cur.fetchone()
    return _row(row)


def delete_mapping(graph_key: str, mapping_id: int) -> bool:
    with _cursor() as cur:
        cur.execute(
            "DELETE FROM custom_class_mapping WHERE graph_key = %s AND id = %s", (graph_key, mapping_id)
        )
        deleted = cur.rowcount > 0
    if not deleted:
        raise MappingNotFoundError(mapping_id)
    return True


# ---------------------------------------------------------------------------
# TTL serialization
# ---------------------------------------------------------------------------


def _ttl_str(text: str) -> str:
    escaped = text.replace("\\", "\\\\").replace('"', '\\"').replace("\r", "").replace("\n", "\\n")
    return f'"{escaped}"'


def serialize_to_ttl(graph_key: str) -> str:
    """Draft rows -> Turtle text. Hand-built (not via rdflib graph
    construction) so the output reads the same one-triple-per-line style as
    the predefined ontology's hand-written TTL. Not re-parsed by
    cphr_ontology_service.py's regex parser (that one's hardcoded to the
    predefined ontology's file) -- this just has to be valid Turtle, checked
    by ontology_publish.validate_ttl() before it's ever saved or published."""
    ontology = get_ontology(graph_key)
    if not ontology:
        raise OntologyNotFoundError(graph_key)

    with _cursor() as cur:
        cur.execute("SELECT * FROM custom_class WHERE graph_key = %s ORDER BY name", (graph_key,))
        classes = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT * FROM custom_property WHERE graph_key = %s ORDER BY name", (graph_key,))
        properties = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT * FROM custom_class_mapping WHERE graph_key = %s ORDER BY id", (graph_key,))
        mappings = [dict(r) for r in cur.fetchall()]

    lines: list[str] = []
    lines.append(f"@prefix cphr: <http://cml.org/ontology/{graph_key}#> .")
    lines.append("@prefix owl: <http://www.w3.org/2002/07/owl#> .")
    lines.append("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .")
    lines.append("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .")
    lines.append("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .")
    lines.append("@prefix skos: <http://www.w3.org/2004/02/skos/core#> .")
    lines.append("@prefix dcterms: <http://purl.org/dc/terms/> .")
    lines.append("@prefix dwc: <http://rs.tdwg.org/dwc/terms/> .")

    external_graph_keys = sorted({m["to_ontology_graph_key"] for m in mappings if m["to_ontology_graph_key"]})
    for ext_key in external_graph_keys:
        lines.append(f"@prefix ext_{ext_key}: <http://cml.org/ontology/{ext_key}#> .")
    lines.append("")

    lines.append("cphr: a owl:Ontology ;")
    lines.append(f"    rdfs:label {_ttl_str(ontology['title'])} ;")
    lines.append(f"    dcterms:title {_ttl_str(ontology['title'])} ;")
    if ontology.get("description"):
        lines.append(f"    dcterms:description {_ttl_str(ontology['description'])} ;")
    lines.append(f"    cphr:acronym {_ttl_str(ontology['acronym'])} ;")
    lines.append(f"    cphr:visibility {_ttl_str(ontology['visibility'])} ;")
    lines.append(f"    cphr:status {_ttl_str(ontology['status'])} ;")
    for category in ontology.get("categories") or []:
        lines.append(f"    cphr:hasCategory {_ttl_str(category)} ;")
    for ref in ontology.get("bibliographic_refs") or []:
        lines.append(f"    dcterms:bibliographicCitation {_ttl_str(ref)} ;")
    if ontology.get("contact"):
        lines.append(f"    dcterms:contributor {_ttl_str(ontology['contact'])} ;")
    lines.append(f'    dcterms:created "{ontology["created_at"].date().isoformat()}" ;')
    lines.append(f'    dcterms:modified "{ontology["updated_at"].date().isoformat()}" .')
    lines.append("")

    lines.append("cphr:acronym a owl:AnnotationProperty .")
    lines.append("cphr:visibility a owl:AnnotationProperty .")
    lines.append("cphr:status a owl:AnnotationProperty .")
    lines.append("cphr:hasCategory a owl:AnnotationProperty .")
    lines.append("")

    for cls in classes:
        lines.append(f"cphr:{cls['name']} a owl:Class ;")
        if cls["parent_name"]:
            lines.append(f"    rdfs:subClassOf cphr:{cls['parent_name']} ;")
        lines.append(f"    rdfs:label {_ttl_str(cls['label'])} ;")
        for synonym in cls.get("synonyms") or []:
            lines.append(f"    skos:altLabel {_ttl_str(synonym)} ;")
        comment_source = cls.get("definition") or cls["label"]
        if cls.get("definition"):
            lines.append(f"    skos:definition {_ttl_str(cls['definition'])} ;")
        lines.append(f"    rdfs:comment {_ttl_str(comment_source)} .")
        lines.append("")

    for prop in properties:
        owl_kind = "DatatypeProperty" if prop["property_type"] == "datatype" else "ObjectProperty"
        range_prefix = "xsd:" if prop["property_type"] == "datatype" else "cphr:"
        lines.append(f"cphr:{prop['name']} a owl:{owl_kind} ;")
        lines.append(f"    rdfs:domain cphr:{prop['domain_class_name']} ;")
        lines.append(f"    rdfs:range {range_prefix}{prop['range_value']} ;")
        lines.append(f"    rdfs:label {_ttl_str(prop['label'])} ;")
        comment_text = prop.get("comment") or prop["label"]
        if prop.get("cardinality_note"):
            comment_text = f"{comment_text} Cardinality: {prop['cardinality_note']}."
        lines.append(f"    rdfs:comment {_ttl_str(comment_text)} .")
        lines.append("")

    for mapping in mappings:
        if mapping["target_kind"] == "internal":
            target = f"cphr:{mapping['to_class_name']}"
        elif mapping["target_kind"] == "external_ontology":
            target = f"ext_{mapping['to_ontology_graph_key']}:{mapping['to_ref']}"
        else:
            target = mapping["to_ref"]
        lines.append(f"cphr:{mapping['from_class_name']} {mapping['relation']} {target} .")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Validate / Publish / Versions
# ---------------------------------------------------------------------------


def validate_ontology(graph_key: str) -> dict:
    from app.ontology_publish import TtlValidationError, validate_ttl

    content = serialize_to_ttl(graph_key)
    try:
        counts = validate_ttl(content)
    except TtlValidationError as exc:
        raise ValidationError(str(exc))
    return {"valid": True, **counts}


async def publish_ontology(graph_key: str) -> dict:
    """Reuses the existing generic publish pipeline (app/ontology_publish.py)
    used by the predefined ontology -- register the graph_key if new, write
    ontology-ttl-files/{graph_key}.ttl, and PUT-replace the Fuseki named
    graph. Overwrites in place; does not create a version (see
    custom_ontology_version's comment)."""
    from app import ontology_publish

    ontology = get_ontology(graph_key)
    if not ontology:
        raise OntologyNotFoundError(graph_key)

    content = serialize_to_ttl(graph_key)
    try:
        counts = ontology_publish.validate_ttl(content)
    except ontology_publish.TtlValidationError as exc:
        raise ValidationError(str(exc))

    ontology_publish.register_graph_key(graph_key, label=ontology["title"])
    saved_path = ontology_publish.save_ttl(graph_key, content)
    fuseki_result = await ontology_publish.push_to_fuseki(graph_key, content)

    with _cursor() as cur:
        cur.execute("UPDATE custom_ontology SET last_published_at = NOW() WHERE graph_key = %s", (graph_key,))

    return {
        "graph_key": graph_key,
        "saved_path": str(saved_path),
        "fuseki": fuseki_result,
        **counts,
    }


def list_versions(graph_key: str) -> list[dict]:
    with _cursor() as cur:
        if not _ontology_exists(cur, graph_key):
            raise OntologyNotFoundError(graph_key)
        cur.execute(
            """
            SELECT id, graph_key, version_no, triple_count, class_count,
                   object_property_count, datatype_property_count, created_at
            FROM custom_ontology_version WHERE graph_key = %s ORDER BY version_no DESC
            """,
            (graph_key,),
        )
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def get_version(graph_key: str, version_no: int) -> dict:
    with _cursor() as cur:
        cur.execute(
            "SELECT * FROM custom_ontology_version WHERE graph_key = %s AND version_no = %s",
            (graph_key, version_no),
        )
        row = cur.fetchone()
    if not row:
        raise VersionNotFoundError(version_no)
    return _row(row)


def create_version(graph_key: str) -> dict:
    """Explicit snapshot of the CURRENT draft (not necessarily the last
    published state) -- a deliberate action distinct from Publish, per the
    2026-09-03 product decision."""
    from app.ontology_publish import TtlValidationError, validate_ttl

    content = serialize_to_ttl(graph_key)
    try:
        counts = validate_ttl(content)
    except TtlValidationError as exc:
        raise ValidationError(str(exc))

    with _cursor() as cur:
        cur.execute(
            "SELECT COALESCE(MAX(version_no), 0) + 1 AS next_version FROM custom_ontology_version WHERE graph_key = %s",
            (graph_key,),
        )
        next_version = cur.fetchone()["next_version"]
        cur.execute(
            """
            INSERT INTO custom_ontology_version
                (graph_key, version_no, ttl_content, triple_count, class_count, object_property_count, datatype_property_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id, graph_key, version_no, triple_count, class_count, object_property_count, datatype_property_count, created_at
            """,
            (
                graph_key, next_version, content, counts["triple_count"], counts["class_count"],
                counts["object_property_count"], counts["datatype_property_count"],
            ),
        )
        row = cur.fetchone()
    return _row(row)
