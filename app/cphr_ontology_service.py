from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import os
import re


_DEFAULT_TTL_PATH = Path("/home/metastring/src/CPHR-Backend/ontology/cphr-ontology.ttl")
_DEFAULT_FIELD_MAP_PATH = Path("/home/metastring/src/CPHR-Backend/ontology/cphr-ontology-field-map.md")

ONTOLOGY_TTL_PATH = Path(os.getenv("CPHR_ONTOLOGY_TTL_PATH", str(_DEFAULT_TTL_PATH)))
ONTOLOGY_FIELD_MAP_PATH = Path(os.getenv("CPHR_ONTOLOGY_FIELD_MAP_PATH", str(_DEFAULT_FIELD_MAP_PATH)))

CPHR_PREFIX = "cphr:"
XSD_PREFIX = "xsd:"


def _normalize_term_name(value: str) -> str:
    if value.startswith(CPHR_PREFIX):
        return value[len(CPHR_PREFIX):]
    return value


def _normalize_range_name(value: str) -> str:
    if value.startswith(CPHR_PREFIX):
        return value[len(CPHR_PREFIX):]
    if value.startswith(XSD_PREFIX):
        return value[len(XSD_PREFIX):]
    return value


def _parse_object_value(value: str) -> str:
    literal_match = re.match(r'^"(?P<text>.*)"(?:\^\^[\w:]+)?$', value)
    if literal_match:
        return literal_match.group("text")
    if value.startswith(CPHR_PREFIX) or value.startswith(XSD_PREFIX):
        return value
    return value


def _load_ttl_entities() -> dict[str, dict]:
    text = ONTOLOGY_TTL_PATH.read_text(encoding="utf-8")
    blocks: list[list[str]] = []
    current_block: list[str] = []

    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line:
            if current_block:
                blocks.append(current_block)
                current_block = []
            continue
        current_block.append(line)

    if current_block:
        blocks.append(current_block)

    entities: dict[str, dict] = {}
    header_pattern = re.compile(r"^cphr:(?P<name>\w*)\s+a\s+owl:(?P<kind>\w+)\s*[;.]$")
    predicate_pattern = re.compile(r"^(?P<predicate>[\w:]+)\s+(?P<object>.+?)\s*[;.]$")

    for block in blocks:
        header = block[0].strip()
        header_match = header_pattern.match(header)
        if not header_match:
            continue

        name = header_match.group("name") or "ontology"
        kind = header_match.group("kind")
        entity = {
            "name": name,
            "kind": kind,
            "predicates": defaultdict(list),
        }

        for line in block[1:]:
            line = line.strip()
            match = predicate_pattern.match(line)
            if not match:
                continue
            predicate = match.group("predicate")
            obj = _parse_object_value(match.group("object"))
            entity["predicates"][predicate].append(obj)

        entities[name] = entity

    return entities


def _parse_field_map() -> dict[str, object]:
    lines = ONTOLOGY_FIELD_MAP_PATH.read_text(encoding="utf-8").splitlines()
    section_title = None
    sections: list[dict[str, object]] = []
    class_index: dict[str, dict[str, object]] = {}

    group_map = {
        "Core biodiversity classes": "biodiversity",
        "Traditional drug and formulation classes": "traditional_medicine",
        "Curation-support classes": "curation_support",
        "Extension-ready classes": "extension",
    }

    line_index = 0
    while line_index < len(lines):
        line = lines[line_index]

        if line.startswith("## "):
            section_title = line[3:].strip()

        if not section_title or not line.startswith("| "):
            line_index += 1
            continue

        if line.startswith("| Class | Covered fields |") and line_index + 1 < len(lines):
            if section_title not in group_map:
                line_index += 1
                continue

            section = {
                "title": section_title,
                "group": group_map[section_title],
                "entries": [],
            }
            sections.append(section)
            line_index += 2

            while line_index < len(lines) and lines[line_index].startswith("|"):
                row = lines[line_index]
                if row.startswith("| ---"):
                    line_index += 1
                    continue

                cells = [cell.strip() for cell in row.strip("|").split("|")]
                if len(cells) < 2:
                    line_index += 1
                    continue

                class_match = re.search(r"`cphr:(?P<name>[^`]+)`", cells[0])
                if not class_match:
                    line_index += 1
                    continue

                class_name = class_match.group("name")
                field_matches = re.findall(r"`([^`]+)`", cells[1])
                free_text = cells[1] if not field_matches else ""

                entry = {
                    "class_name": class_name,
                    "covered_fields": field_matches,
                    "description": free_text,
                }
                section["entries"].append(entry)

                if class_name not in class_index:
                    class_index[class_name] = {
                        "group": group_map[section_title],
                        "section_title": section_title,
                        "covered_fields": [],
                        "descriptions": [],
                    }

                class_index[class_name]["covered_fields"].extend(field_matches)
                if free_text:
                    class_index[class_name]["descriptions"].append(free_text)

                line_index += 1

            continue

        line_index += 1

    for class_info in class_index.values():
        unique_fields = []
        seen_fields = set()
        for field_name in class_info["covered_fields"]:
            if field_name not in seen_fields:
                unique_fields.append(field_name)
                seen_fields.add(field_name)
        class_info["covered_fields"] = unique_fields

    return {
        "sections": sections,
        "class_index": class_index,
    }


def _build_snapshot() -> dict[str, object]:
    entities = _load_ttl_entities()
    field_map = _parse_field_map()

    classes = []
    datatype_properties = []
    object_properties = []

    for name, entity in entities.items():
        predicates = entity["predicates"]

        if entity["kind"] == "Class":
            class_info = field_map["class_index"].get(name, {})
            classes.append(
                {
                    "name": name,
                    "label": predicates.get("rdfs:label", [name])[0],
                    "comment": predicates.get("rdfs:comment", [""])[0],
                    "parents": [_normalize_term_name(value) for value in predicates.get("rdfs:subClassOf", [])],
                    "group": class_info.get("group", "other"),
                    "section_title": class_info.get("section_title"),
                    "covered_fields": class_info.get("covered_fields", []),
                    "descriptions": class_info.get("descriptions", []),
                    "design_notes": predicates.get("cphr:designNote", []),
                }
            )
        elif entity["kind"] == "DatatypeProperty":
            datatype_properties.append(
                {
                    "name": name,
                    "label": predicates.get("rdfs:label", [name])[0],
                    "comment": predicates.get("rdfs:comment", [""])[0],
                    "property_type": "datatype",
                    "domains": [_normalize_term_name(value) for value in predicates.get("rdfs:domain", [])],
                    "ranges": [_normalize_range_name(value) for value in predicates.get("rdfs:range", [])],
                    "design_notes": predicates.get("cphr:designNote", []),
                }
            )
        elif entity["kind"] == "ObjectProperty":
            object_properties.append(
                {
                    "name": name,
                    "label": predicates.get("rdfs:label", [name])[0],
                    "comment": predicates.get("rdfs:comment", [""])[0],
                    "property_type": "object",
                    "domains": [_normalize_term_name(value) for value in predicates.get("rdfs:domain", [])],
                    "ranges": [_normalize_range_name(value) for value in predicates.get("rdfs:range", [])],
                    "design_notes": predicates.get("cphr:designNote", []),
                }
            )

    classes.sort(key=lambda item: item["name"])
    datatype_properties.sort(key=lambda item: item["name"])
    object_properties.sort(key=lambda item: item["name"])

    class_by_name = {item["name"]: item for item in classes}
    property_by_name = {item["name"]: item for item in datatype_properties + object_properties}

    children_by_parent: dict[str, list[str]] = defaultdict(list)
    for class_info in classes:
        for parent_name in class_info["parents"]:
            children_by_parent[parent_name].append(class_info["name"])

    ontology_metadata = entities.get("ontology", {"predicates": defaultdict(list)})["predicates"]

    summary = {
        "title": ontology_metadata.get("dcterms:title", ["CPHR Ontology"])[0],
        "description": ontology_metadata.get("dcterms:description", [""])[0],
        "created": ontology_metadata.get("dcterms:created", [""])[0],
        "class_count": len(classes),
        "object_property_count": len(object_properties),
        "datatype_property_count": len(datatype_properties),
        "groups": {
            group_name: len([item for item in classes if item["group"] == group_name])
            for group_name in sorted({item["group"] for item in classes})
        },
    }

    return {
        "summary": summary,
        "classes": classes,
        "class_by_name": class_by_name,
        "children_by_parent": {key: sorted(value) for key, value in children_by_parent.items()},
        "datatype_properties": datatype_properties,
        "object_properties": object_properties,
        "property_by_name": property_by_name,
        "field_map": field_map,
    }


def load_ontology_snapshot() -> dict[str, object]:
    return _build_snapshot()


def get_class_detail(class_name: str) -> dict[str, object] | None:
    normalized_name = _normalize_term_name(class_name)
    snapshot = load_ontology_snapshot()
    class_info = snapshot["class_by_name"].get(normalized_name)
    if not class_info:
        return None

    datatype_properties = []
    seen_datatype_properties = set()
    for property_name in class_info["covered_fields"]:
        property_info = snapshot["property_by_name"].get(property_name)
        if property_info and property_info["property_type"] == "datatype":
            datatype_properties.append(property_info)
            seen_datatype_properties.add(property_name)

    for property_info in snapshot["datatype_properties"]:
        if normalized_name in property_info["domains"] and property_info["name"] not in seen_datatype_properties:
            datatype_properties.append(property_info)
            seen_datatype_properties.add(property_info["name"])

    outgoing_object_properties = [
        property_info
        for property_info in snapshot["object_properties"]
        if normalized_name in property_info["domains"]
    ]
    incoming_object_properties = [
        property_info
        for property_info in snapshot["object_properties"]
        if normalized_name in property_info["ranges"]
    ]

    return {
        **class_info,
        "children": snapshot["children_by_parent"].get(normalized_name, []),
        "datatype_properties": sorted(datatype_properties, key=lambda item: item["name"]),
        "outgoing_object_properties": sorted(outgoing_object_properties, key=lambda item: item["name"]),
        "incoming_object_properties": sorted(incoming_object_properties, key=lambda item: item["name"]),
    }


def search_ontology(query: str) -> dict[str, list[dict[str, object]]]:
    search_text = query.strip().lower()
    snapshot = load_ontology_snapshot()

    def matches(record: dict[str, object]) -> bool:
        haystack = " ".join(
            str(value)
            for key, value in record.items()
            if key in {"name", "label", "comment"}
        ).lower()
        return search_text in haystack

    return {
        "classes": [item for item in snapshot["classes"] if matches(item)],
        "properties": [
            item
            for item in snapshot["datatype_properties"] + snapshot["object_properties"]
            if matches(item)
        ],
    }


def build_graph_view() -> dict[str, object]:
    snapshot = load_ontology_snapshot()

    nodes = [
        {
            "id": class_info["name"],
            "label": class_info["label"],
            "group": class_info["group"],
            "type": "class",
        }
        for class_info in snapshot["classes"]
    ]

    edges = []
    for class_info in snapshot["classes"]:
        for parent_name in class_info["parents"]:
            edges.append(
                {
                    "source": class_info["name"],
                    "target": parent_name,
                    "label": "subClassOf",
                    "type": "inheritance",
                }
            )

    unresolved_object_properties = []
    for property_info in snapshot["object_properties"]:
        if property_info["domains"] and property_info["ranges"]:
            for domain_name in property_info["domains"]:
                for range_name in property_info["ranges"]:
                    edges.append(
                        {
                            "source": domain_name,
                            "target": range_name,
                            "label": property_info["label"],
                            "property_name": property_info["name"],
                            "type": "object_property",
                        }
                    )
        else:
            unresolved_object_properties.append(property_info)

    return {
        "nodes": nodes,
        "edges": edges,
        "unresolved_object_properties": unresolved_object_properties,
    }
