import asyncio
import importlib

from fastapi.testclient import TestClient


def test_ping_endpoint_returns_pong():
    """Basic health check for the FastAPI app /ping endpoint."""
    import main as main
    client = TestClient(main.app)

    response = client.get("/ping")

    assert response.status_code == 200
    assert response.json() == {"ping": "pong"}


def test_normalize_dataset_name_trims_and_lowercases():
    """_normalize_dataset_name should trim, lowercase and normalise apostrophes."""

    import main as main

    value = "  Citizens’ Portal of Medicinal Plants  "
    result = main._normalize_dataset_name(value)

    assert result == "citizens' portal of medicinal plants"


def test_canonical_field_name_maps_recipe_to_recipe_name():
    """_canonical_field_name should map the recipe alias to recipe_name."""

    import main as main

    assert main._canonical_field_name("recipe") == "recipe_name"
    assert main._canonical_field_name("Recipe") == "recipe_name"
    assert main._canonical_field_name("other") == "other"


def test_project_biodiversity_result_preserves_ids_and_requested_field():
    """_project_biodiversity_result keeps *_id columns and requested field only."""

    import main as main

    row = {
        "taxon_id": 1,
        "id": 2,
        "taxon_name": "Panthera leo",
        "other": "ignored",
    }

    projected = main._project_biodiversity_result(row, "taxon_name")

    # ID-like columns are preserved
    assert projected["taxon_id"] == 1
    assert projected["id"] == 2
    # Requested field is kept
    assert projected["taxon_name"] == "Panthera leo"
    # Unrelated fields are dropped
    assert "other" not in projected


def test_participants_use_default_urls_when_env_not_set(monkeypatch):
    """PARTICIPANTS should contain the expected default URLs when env is unset."""

    # Ensure participant-related env vars are not set
    for key in [
        "PARTICIPANT_KEW_PLANT_DB_URL",
        "PARTICIPANT_CPMP_CITIZENS_PORTAL_URL",
        "PARTICIPANT_CPMP_BOTANICAL_SOURCE_URL",
        "PARTICIPANT_CPMP_DRUG_SOURCE_URL",
        "PARTICIPANT_TMIP_URL",
        "PARTICIPANT_AYURAHAAR_URL",
        "PARTICIPANT_RASASHASTRA_URL",
    ]:
        monkeypatch.delenv(key, raising=False)

    import main as main

    assert main.PARTICIPANTS["Kew Plant Database"] == "http://134.209.145.106:8000/search"
    assert main.PARTICIPANTS["Citizens’ Portal of Medicinal Plants"] == "http://139.59.84.243:8050/search"
    assert main.PARTICIPANTS["CPMP Botanical Source"] == "http://139.59.84.243:8050/search"
    assert main.PARTICIPANTS["CPMP Drug Source"] == "http://139.59.84.243:9087/search/search/drugname"


def test_participants_can_be_overridden_via_env(monkeypatch):
    """Participant URLs should honour environment variable overrides."""

    monkeypatch.setenv("PARTICIPANT_KEW_PLANT_DB_URL", "http://example.com/kew")

    import main as main

    assert main.PARTICIPANTS["Kew Plant Database"] == "http://example.com/kew"


def test_fetch_from_participant_cpmp_botanical_source_uses_keyword_payload():
    """CPMP Botanical Source should perform a POST with keyword, page, size, and taxon_status."""

    import main as main

    class DummyResponse:
        def __init__(self, payload):
            self.payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self.payload

    class DummyClient:
        def __init__(self):
            self.last_request = None

        async def post(self, url, json=None):
            self.last_request = {"url": url, "json": json}
            return DummyResponse({
                "searchResults": [
                    {
                        "taxonId": 16036,
                        "taxonName": "abelia chinensis r.br.",
                        "commonNames": None,
                    }
                ]
            })

    client = DummyClient()
    result = asyncio.run(
        main.fetch_from_participant(
            client,
            "CPMP Botanical Source",
            "https://cpmp.tdu.edu.in/api/species/search/v2",
            "scientific_name",
            "abel",
        )
    )

    assert client.last_request["url"] == "https://cpmp.tdu.edu.in/api/species/search/v2"
    assert client.last_request["json"]["keyword"] == "abel"
    assert client.last_request["json"]["page"] == "1"
    assert client.last_request["json"]["size"] == "25"
    assert client.last_request["json"]["taxon_status"] == ["Accepted"]
    assert result["results"][0]["scientific_name"] == "abelia chinensis r.br."


def test_local_name_extracts_fragment_or_last_path_segment():
    """_local_name should prefer fragment then last path segment for URIs."""

    import app.endpoints.ontology as ontology

    assert ontology._local_name("http://cml.org/ontology#Dataset") == "Dataset"
    assert (
        ontology._local_name("http://cml.org/ontology/metadata/Dataset")
        == "Dataset"
    )
    assert ontology._local_name("plain_literal") == "plain_literal"


def test_build_triples_query_includes_limit_value():
    """build_triples_query should embed the requested LIMIT value."""

    import app.endpoints.ontology as ontology

    query = ontology.build_triples_query(123)

    assert "LIMIT 123" in query


def test_fuseki_sparql_endpoint_default_and_override(monkeypatch):
    """FUSEKI_SPARQL_ENDPOINT should have a sensible default and be overridable."""

    # Default value case
    monkeypatch.delenv("FUSEKI_SPARQL_ENDPOINT", raising=False)
    import app.endpoints.ontology as ontology
    assert (
        ontology.FUSEKI_SPARQL_ENDPOINT
        == "http://139.59.84.243:3030/cml-ontology/query"
    )

    # Override via environment
    monkeypatch.setenv("FUSEKI_SPARQL_ENDPOINT", "http://example.com/fuseki")
    ontology_overridden = reload_module("app.endpoints.ontology")
    assert ontology_overridden.FUSEKI_SPARQL_ENDPOINT == "http://example.com/fuseki"
