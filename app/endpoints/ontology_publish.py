from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import PlainTextResponse

from app.ontology_publish import (
    InvalidGraphKeyError,
    OntologyKeyError,
    TtlValidationError,
    delete_graph_from_fuseki,
    delete_ttl_file,
    deregister_graph_key,
    get_registry,
    push_to_fuseki,
    read_ttl,
    register_graph_key,
    save_ttl,
    validate_ttl,
)

router = APIRouter(prefix="/ontology/ttl", tags=["Ontology TTL Publishing"])


@router.get("")
def list_ontology_files():
    """List the known ontology TTL files this interim update mechanism manages,
    and whether each currently exists on disk."""
    items = []
    for graph_key, entry in get_registry().items():
        file_path = entry["file_path"]
        items.append(
            {
                "graph_key": graph_key,
                "label": entry["label"],
                "fuseki_graph": entry["fuseki_graph"],
                "file_path": str(file_path),
                "exists": file_path.exists(),
            }
        )
    return {"count": len(items), "items": items}


@router.get("/{graph_key}", response_class=PlainTextResponse)
def download_ontology_ttl(graph_key: str):
    """Return the current raw TTL content for graph_key, so a teammate can pull
    the latest before editing it further."""
    try:
        return read_ttl(graph_key)
    except OntologyKeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No TTL file on disk yet for {graph_key!r}")


@router.post("/{graph_key}")
async def upload_ontology_ttl(
    graph_key: str,
    file: UploadFile = File(...),
    label: str | None = Form(
        default=None,
        description="Display label, only used the first time graph_key is created.",
    ),
    dry_run: bool = Query(
        default=False,
        description="If true, only validate — do not write the file or push to Fuseki.",
    ),
):
    """Upload a TTL for graph_key: validate as Turtle, then (unless dry_run)
    overwrite the tracked file and replace the matching Fuseki named graph.

    graph_key does not need to already exist — if it's new, this both creates
    the registry entry (file at ontology-ttl-files/{graph_key}.ttl, Fuseki
    graph http://cml.org/ontology/{graph_key}) and populates it in one call.
    The file-based ontology API and the live SPARQL endpoint both reflect the
    update immediately after (no app restart needed — both re-read on every
    request).
    """
    raw = await file.read()
    try:
        content = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File is not valid UTF-8 text")

    try:
        counts = validate_ttl(content)
    except TtlValidationError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid Turtle: {exc}")

    if dry_run:
        return {"graph_key": graph_key, "dry_run": True, "valid": True, **counts}

    try:
        entry = register_graph_key(graph_key, label=label)
    except InvalidGraphKeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    try:
        saved_path = save_ttl(graph_key, content)
    except OSError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Validated OK, but could not write the file (permissions?): {exc}",
        )

    try:
        fuseki_result = await push_to_fuseki(graph_key, content)
    except Exception as exc:
        # File is already saved at this point — the file-based API (which
        # re-reads on every call) is up to date even if the live SPARQL graph
        # push failed, so surface this as a partial-success error rather than
        # silently losing the write or pretending Fuseki is in sync.
        raise HTTPException(
            status_code=502,
            detail=(
                f"TTL saved to {saved_path}, but pushing to Fuseki failed: {exc}. "
                "The file-based ontology API reflects the new content; Fuseki does not yet."
            ),
        )

    return {
        "graph_key": graph_key,
        "label": entry["label"],
        "dry_run": False,
        "valid": True,
        "saved_path": str(saved_path),
        "fuseki": fuseki_result,
        **counts,
    }


@router.delete("/{graph_key}")
async def delete_ontology_ttl(graph_key: str):
    """Remove graph_key entirely: drop its Fuseki named graph, delete its local
    TTL file if present, and deregister it. If the Fuseki drop fails for a real
    reason (not just "already gone"), nothing else is touched — the registry
    entry stays valid so this can be retried."""
    try:
        fuseki_result = await delete_graph_from_fuseki(graph_key)
    except OntologyKeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Could not drop the Fuseki graph for {graph_key!r}: {exc}. Nothing else was removed.",
        )

    removed_file = delete_ttl_file(graph_key)
    deregister_graph_key(graph_key)

    return {
        "graph_key": graph_key,
        "removed_file": str(removed_file) if removed_file else None,
        "fuseki": fuseki_result,
    }
