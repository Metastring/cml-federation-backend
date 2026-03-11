from fastapi import APIRouter, HTTPException
import httpx
from app.db import get_connection
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/ontology", tags=["Ontology"])

# Separate router for biodiversity ontology mappings
biodiversity_router = APIRouter(prefix="/biodiversity/ontology", tags=["Biodiversity Ontology"])

FUSEKI_SPARQL_ENDPOINT = "http://139.59.84.243:3030/cml-ontology/query"


def _local_name(uri_or_literal: str) -> str:
    """Return a short, frontend-friendly name for a URI.

    - If it's a URI with a '#', use the part after '#'
    - Else if it has '/', use the part after the last '/'
    - Otherwise return the string as-is
    """

    if not isinstance(uri_or_literal, str):
        return str(uri_or_literal)

    if "#" in uri_or_literal:
        return uri_or_literal.rsplit("#", 1)[-1]
    if "/" in uri_or_literal:
        return uri_or_literal.rstrip("/").rsplit("/", 1)[-1]
    return uri_or_literal

# Basic query: get all distinct nodes appearing in any named graph
SPARQL_ALL_NODES = """
SELECT DISTINCT ?node
WHERE {
    GRAPH ?g {
        ?node ?p ?o .
    }
}
"""

# Nodes belonging specifically to the CML Metadata Ontology
SPARQL_METADATA_NODES = """
SELECT DISTINCT ?node
WHERE {
    GRAPH ?g {
        ?node ?p ?o .
        FILTER(STRSTARTS(STR(?node), "http://cml.org/metadata#"))
    }
}
"""

# Edges (domain -> range) for metadata ontology object properties
SPARQL_METADATA_EDGES = """
SELECT DISTINCT ?source ?target ?prop
WHERE {
    GRAPH ?g {
        ?prop a <http://www.w3.org/2002/07/owl#ObjectProperty> ;
              <http://www.w3.org/2000/01/rdf-schema#domain> ?source ;
              <http://www.w3.org/2000/01/rdf-schema#range> ?target .
        FILTER(STRSTARTS(STR(?source), "http://cml.org/metadata#"))
        FILTER(STRSTARTS(STR(?target), "http://cml.org/metadata#"))
    }
}
"""

# Simple term view: classes/properties with label, parent, description
SPARQL_TERMS = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

SELECT DISTINCT ?term ?type ?label ?parent ?comment
WHERE {
    GRAPH ?g {
        ?term a ?type .
        FILTER(STRSTARTS(STR(?term), "http://cml.org/"))

        OPTIONAL { ?term rdfs:label ?label . }
        OPTIONAL { ?term rdfs:subClassOf ?parent . }
        OPTIONAL { ?term rdfs:subPropertyOf ?parent . }
        OPTIONAL { ?term rdfs:comment ?comment . }
    }
}
"""


@biodiversity_router.get("/classes")
def get_biodiversity_ontology_classes():
    """Return all class-related mappings from biodiversity_ontology_mapping.

    This includes rows where ontology_element_type represents a class
    (e.g. "Class", "Class Instance").
    """

    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            SELECT
                dataset_area,
                dataset_column,
                ontology_element_type,
                ontology_element,
                description
            FROM biodiversity_ontology_mapping
            WHERE ontology_element_type IN ('Class', 'Class Instance')
            ORDER BY dataset_area, dataset_column;
            """
        )
        rows = cursor.fetchall() or []
        return {
            "count": len(rows),
            "items": rows,
        }
    finally:
        conn.close()


@biodiversity_router.get("/properties")
def get_biodiversity_ontology_properties():
    """Return all property-related mappings from biodiversity_ontology_mapping.

    This includes rows where ontology_element_type is an object or
    datatype property.
    """

    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            SELECT
                dataset_area,
                dataset_column,
                ontology_element_type,
                ontology_element,
                description
            FROM biodiversity_ontology_mapping
            WHERE ontology_element_type IN ('Object Property', 'Datatype Property')
            ORDER BY dataset_area, dataset_column;
            """
        )
        rows = cursor.fetchall() or []
        return {
            "count": len(rows),
            "items": rows,
        }
    finally:
        conn.close()


@biodiversity_router.get("/all")
def get_biodiversity_ontology_mappings():
    """Return all rows from biodiversity_ontology_mapping.

    This gives the full mapping of dataset areas/columns to
    ontology elements and descriptions.
    """

    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            SELECT
                dataset_area,
                dataset_column,
                ontology_element_type,
                ontology_element,
                description
            FROM biodiversity_ontology_mapping
            ORDER BY dataset_area, dataset_column;
            """
        )
        rows = cursor.fetchall() or []
        return {
            "count": len(rows),
            "items": rows,
        }
    finally:
        conn.close()

# Query: get subject/predicate/object triples from all graphs.
# We will inject the LIMIT value using an f-string in the
# endpoint to avoid issues with Python's .format and braces.
def build_triples_query(limit: int) -> str:
    return f"""
SELECT ?g ?s ?p ?o
WHERE {{
    GRAPH ?g {{
        ?s ?p ?o .
    }}
}}
LIMIT {limit}
"""


@router.get("/nodes")
async def get_ontology_nodes():
    """Return all distinct ontology nodes from the Fuseki dataset.

    This proxies a SPARQL query to the Apache Jena Fuseki server
    running on the same machine and returns the list of node URIs.
    """

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                FUSEKI_SPARQL_ENDPOINT,
                data={"query": SPARQL_ALL_NODES},
                headers={"Accept": "application/sparql-results+json"},
            )
            response.raise_for_status()
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Error contacting Fuseki: {exc}")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail=f"Fuseki returned error: {exc.response.text}")

    data = response.json()
    bindings = data.get("results", {}).get("bindings", [])

    nodes = []
    for binding in bindings:
        node = binding.get("node") or {}
        value = node.get("value")
        if value is None:
            continue
        nodes.append(
            {
                "uri": value,
                "local_name": _local_name(value),
            }
        )

    return {
        "count": len(nodes),
        "nodes": nodes,
    }


@router.get("/metadata/nodes")
async def get_metadata_ontology_nodes():
    """Return all distinct nodes from the CML Metadata Ontology.

    This filters nodes whose URI starts with the metadata prefix
    http://cml.org/metadata# so you only get the metadata model
    (Dataset, GovernanceMetadata, fields like dataOwner, etc.).
    """

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                FUSEKI_SPARQL_ENDPOINT,
                data={"query": SPARQL_METADATA_NODES},
                headers={"Accept": "application/sparql-results+json"},
            )
            response.raise_for_status()
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Error contacting Fuseki: {exc}")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail=f"Fuseki returned error: {exc.response.text}")

    data = response.json()
    bindings = data.get("results", {}).get("bindings", [])

    nodes = []
    for binding in bindings:
        node = binding.get("node") or {}
        value = node.get("value")
        if value is None:
            continue
        nodes.append(
            {
                "uri": value,
                "local_name": _local_name(value),
            }
        )

    return {
        "count": len(nodes),
        "nodes": nodes,
    }


@router.get("/metadata/graph")
async def get_metadata_ontology_graph():
    """Return nodes and edges for the CML Metadata Ontology.

    - Nodes: all metadata URIs (classes + properties) with local_name.
    - Edges: for each owl:ObjectProperty with rdfs:domain and
      rdfs:range in the metadata namespace, create an edge:
        source = domain, target = range, label = local_name(property).
    """

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            # Fetch nodes
            resp_nodes = await client.post(
                FUSEKI_SPARQL_ENDPOINT,
                data={"query": SPARQL_METADATA_NODES},
                headers={"Accept": "application/sparql-results+json"},
            )
            resp_nodes.raise_for_status()

            # Fetch edges
            resp_edges = await client.post(
                FUSEKI_SPARQL_ENDPOINT,
                data={"query": SPARQL_METADATA_EDGES},
                headers={"Accept": "application/sparql-results+json"},
            )
            resp_edges.raise_for_status()
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Error contacting Fuseki: {exc}")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail=f"Fuseki returned error: {exc.response.text}")

    data_nodes = resp_nodes.json()
    data_edges = resp_edges.json()

    bindings_nodes = data_nodes.get("results", {}).get("bindings", [])
    bindings_edges = data_edges.get("results", {}).get("bindings", [])

    nodes = []
    for b in bindings_nodes:
        node = b.get("node") or {}
        value = node.get("value")
        if value is None:
            continue
        nodes.append(
            {
                "uri": value,
                "local_name": _local_name(value),
            }
        )

    edges = []
    for b in bindings_edges:
        source = (b.get("source") or {}).get("value")
        target = (b.get("target") or {}).get("value")
        prop = (b.get("prop") or {}).get("value")
        if not source or not target or not prop:
            continue
        edges.append(
            {
                "source": source,
                "target": target,
                "label": _local_name(prop),
            }
        )

    return {
        "nodes": nodes,
        "edges": edges,
    }


@router.get("/triples")
async def get_ontology_triples(limit: int = 1000):
    """Return RDF triples (subject, predicate, object) from the ontology.

    - Reads from all named graphs in the Fuseki dataset.
    - Returns a list of triples in a simple JSON format that
      is easy for the frontend to consume.
    """

    # Protect the server from unreasonable limits
    if limit <= 0:
        limit = 100
    if limit > 10000:
        limit = 10000

    sparql = build_triples_query(limit)

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                FUSEKI_SPARQL_ENDPOINT,
                data={"query": sparql},
                headers={"Accept": "application/sparql-results+json"},
            )
            response.raise_for_status()
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Error contacting Fuseki: {exc}")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail=f"Fuseki returned error: {exc.response.text}")

    data = response.json()
    bindings = data.get("results", {}).get("bindings", [])

    triples = []
    for b in bindings:
        s = b.get("s", {})
        p = b.get("p", {})
        o = b.get("o", {})
        g = b.get("g", {})

        def as_node(node_binding):
            if not node_binding:
                return None
            value = node_binding.get("value")
            return {
                "value": value,
                "local_name": _local_name(value) if value is not None else None,
                "type": node_binding.get("type"),
                "datatype": node_binding.get("datatype"),
                "lang": node_binding.get("xml:lang"),
            }

        triples.append(
            {
                "graph": as_node(g),
                "subject": as_node(s),
                "predicate": as_node(p),
                "object": as_node(o),
            }
        )

    return {
        "count": len(triples),
        "triples": triples,
    }


@router.get("/terms")
async def get_ontology_terms():
    """Return ontology terms in a frontend-friendly format.

    For each CML ontology term this returns:
    - label: rdfs:label (or local name if missing)
    - type: "class" or "property" (based on rdf:type)
    - parents: list of parent URIs (subClassOf / subPropertyOf) with local_name
    - description: rdfs:comment, if available
    """

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                FUSEKI_SPARQL_ENDPOINT,
                data={"query": SPARQL_TERMS},
                headers={"Accept": "application/sparql-results+json"},
            )
            response.raise_for_status()
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Error contacting Fuseki: {exc}")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail=f"Fuseki returned error: {exc.response.text}")

    data = response.json()
    bindings = data.get("results", {}).get("bindings", [])

    terms_map = {}

    for b in bindings:
        term_node = b.get("term") or {}
        term_uri = term_node.get("value")
        if not term_uri:
            continue

        entry = terms_map.get(term_uri)
        if entry is None:
            entry = {
                "uri": term_uri,
                "local_name": _local_name(term_uri),
                "label": None,
                "type": None,
                "parents": set(),
                "description": None,
            }
            terms_map[term_uri] = entry

        # Label
        label_node = b.get("label") or {}
        label_val = label_node.get("value")
        if label_val and not entry["label"]:
            entry["label"] = label_val

        # Description / comment
        comment_node = b.get("comment") or {}
        comment_val = comment_node.get("value")
        if comment_val and not entry["description"]:
            entry["description"] = comment_val

        # Parent (either subClassOf or subPropertyOf)
        parent_node = b.get("parent") or {}
        parent_uri = parent_node.get("value")
        if parent_uri:
            entry["parents"].add(parent_uri)

        # Type mapping to "class" / "property"
        type_node = b.get("type") or {}
        type_uri = type_node.get("value")
        if type_uri and not entry["type"]:
            if type_uri.endswith("Class"):
                entry["type"] = "class"
            elif type_uri.endswith("Property"):
                entry["type"] = "property"
            else:
                entry["type"] = "other"

    terms = []
    for term_uri, entry in terms_map.items():
        parents = [
            {
                "uri": p,
                "local_name": _local_name(p),
            }
            for p in sorted(entry["parents"])
        ]

        terms.append(
            {
                "uri": term_uri,
                "local_name": entry["local_name"],
                "label": entry["label"] or entry["local_name"],
                "type": entry["type"],
                "parents": parents,
                "description": entry["description"],
            }
        )

    # Sort by label for stable, user-friendly ordering
    terms.sort(key=lambda t: (t["label"] or "").lower())

    return {
        "count": len(terms),
        "terms": terms,
    }
