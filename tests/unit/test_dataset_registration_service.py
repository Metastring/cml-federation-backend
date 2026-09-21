import io

from openpyxl import Workbook


def test_parse_csv_fields_pairs_headers_with_first_row_samples():
    import app.dataset_registration_service as svc

    text = "species_name,obs_date,lat\nPanthera tigris,2025-03-14,11.4102\n"

    fields = svc.parse_csv_fields(text)

    assert fields == [
        {"field_name": "species_name", "sample_value": "Panthera tigris"},
        {"field_name": "obs_date", "sample_value": "2025-03-14"},
        {"field_name": "lat", "sample_value": "11.4102"},
    ]


def test_parse_csv_fields_handles_header_only_file():
    import app.dataset_registration_service as svc

    fields = svc.parse_csv_fields("species_name,obs_date\n")

    assert fields == [
        {"field_name": "species_name", "sample_value": None},
        {"field_name": "obs_date", "sample_value": None},
    ]


def test_parse_xlsx_fields_reads_header_and_first_data_row():
    import app.dataset_registration_service as svc

    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["species_name", "obs_date", "lat"])
    sheet.append(["Panthera tigris", "2025-03-14", 11.4102])
    buffer = io.BytesIO()
    workbook.save(buffer)

    fields = svc.parse_xlsx_fields(buffer.getvalue())

    assert fields == [
        {"field_name": "species_name", "sample_value": "Panthera tigris"},
        {"field_name": "obs_date", "sample_value": "2025-03-14"},
        {"field_name": "lat", "sample_value": "11.4102"},
    ]


def test_parse_sql_dump_tables_extracts_columns_and_skips_constraints():
    import app.dataset_registration_service as svc

    dump = """
    CREATE TABLE forest_health (
        id INTEGER PRIMARY KEY,
        plot_code VARCHAR(64) NOT NULL,
        canopy_cover NUMERIC(5,2),
        UNIQUE (plot_code),
        CONSTRAINT chk_cover CHECK (canopy_cover >= 0)
    );
    """

    tables = svc.parse_sql_dump_tables(dump)

    assert len(tables) == 1
    assert tables[0]["table_name"] == "forest_health"
    columns = {c["field_name"]: c["data_type"] for c in tables[0]["columns"]}
    assert columns["id"] == "INTEGER PRIMARY KEY"
    assert columns["plot_code"] == "VARCHAR(64) NOT NULL"
    assert columns["canopy_cover"] == "NUMERIC(5,2)"
    assert "plot_code" not in [c for c in columns if c == "UNIQUE"]  # constraint line not treated as a column


def test_parse_sql_dump_tables_handles_multiple_tables():
    import app.dataset_registration_service as svc

    dump = (
        "CREATE TABLE a (id INTEGER, name TEXT);\n"
        "CREATE TABLE IF NOT EXISTS public.b (id INTEGER, value NUMERIC(10,2));\n"
    )

    tables = svc.parse_sql_dump_tables(dump)

    assert [t["table_name"] for t in tables] == ["a", "b"]
    assert [c["field_name"] for c in tables[1]["columns"]] == ["id", "value"]


def test_parse_sql_dump_tables_returns_empty_list_when_no_create_table():
    import app.dataset_registration_service as svc

    assert svc.parse_sql_dump_tables("INSERT INTO x VALUES (1);") == []


def test_normalize_field_name_collapses_case_and_separators():
    import app.dataset_registration_service as svc

    assert svc._normalize_field_name("scientific_name") == "scientificname"
    assert svc._normalize_field_name("Scientific Name") == "scientificname"
    assert svc._normalize_field_name("scientific-name") == "scientificname"


def test_best_match_prefers_exact_name_over_partial_label_match():
    import app.dataset_registration_service as svc

    ontology_fields = [
        {"value": "scientificName", "label": "Scientific Name"},
        {"value": "eventDate", "label": "Event Date"},
        {"value": "decimalLatitude", "label": "Latitude"},
    ]

    best, score = svc._best_match("scientific_name", ontology_fields)

    assert best["value"] == "scientificName"
    assert score >= svc.AUTO_THRESHOLD


def test_best_match_returns_low_score_for_unrelated_field():
    import app.dataset_registration_service as svc

    ontology_fields = [
        {"value": "scientificName", "label": "Scientific Name"},
        {"value": "eventDate", "label": "Event Date"},
    ]

    best, score = svc._best_match("observer_notes", ontology_fields)

    assert score < svc.SUGGEST_THRESHOLD


# --- v2 reference-based sources -------------------------------------------

def test_reference_uri_scheme_recognises_object_store_and_network_schemes():
    import app.dataset_registration_service as svc

    assert svc._reference_uri_scheme("s3://bucket/key.csv") == "s3"
    assert svc._reference_uri_scheme("nfs://host/share/file.csv") == "nfs"
    assert svc._reference_uri_scheme("https://api.example.org/data") == "https"


def test_reference_uri_scheme_returns_empty_string_for_bare_path():
    import app.dataset_registration_service as svc

    assert svc._reference_uri_scheme("/mnt/data/species_survey.csv") == ""


def test_split_schema_table_defaults_to_public_when_no_schema_given():
    import app.dataset_registration_service as svc

    assert svc._split_schema_table("plot_observations") == ("public", "plot_observations")


def test_split_schema_table_respects_explicit_schema():
    import app.dataset_registration_service as svc

    assert svc._split_schema_table("forest.plot_observations") == ("forest", "plot_observations")


def test_redact_connection_string_masks_password_only():
    import app.dataset_registration_service as svc

    redacted = svc._redact_connection_string("postgres://readonly:s3cr3t@db.atree.org:5432/forest_health")

    assert "s3cr3t" not in redacted
    assert redacted == "postgres://readonly:****@db.atree.org:5432/forest_health"


def test_redact_connection_string_leaves_password_free_strings_untouched():
    import app.dataset_registration_service as svc

    conn = "postgres://db.atree.org:5432/forest_health"
    assert svc._redact_connection_string(conn) == conn
    assert svc._redact_connection_string(None) is None


def test_capabilities_request_params_maps_display_labels_to_service_codes():
    import app.dataset_registration_service as svc

    assert svc._capabilities_request_params("Vector (WFS)") == {"service": "WFS", "request": "GetCapabilities"}
    assert svc._capabilities_request_params("Raster (WMS)") == {"service": "WMS", "request": "GetCapabilities"}
    assert svc._capabilities_request_params("Tiles (WMTS)") == {"service": "WMTS", "request": "GetCapabilities"}
    assert svc._capabilities_request_params(None) == {"service": "WMS", "request": "GetCapabilities"}


def test_reference_type_label_includes_scheme_for_file_references():
    import app.dataset_registration_service as svc

    assert svc._reference_type_label("file", "s3://bucket/key.csv") == "File path (s3://)"
    assert svc._reference_type_label("file", None) == "File path"
    assert svc._reference_type_label("url") == "URL / API"
    assert svc._reference_type_label("database") == "Database"
    assert svc._reference_type_label("map_service") == "Map service"
    assert svc._reference_type_label(None) is None


def test_reference_location_picks_field_by_source_type_and_never_leaks_connection_string():
    import app.dataset_registration_service as svc

    assert svc._reference_location(None) is None
    assert svc._reference_location({"source_type": "file", "reference_uri": "s3://x/y.csv"}) == "s3://x/y.csv"
    assert svc._reference_location({"source_type": "url", "source_url": "https://api.example.org"}) == "https://api.example.org"
    assert svc._reference_location({"source_type": "database", "selected_table_name": "public.plots", "db_connection_string": "postgres://u:p@h/db"}) == "public.plots"
    assert svc._reference_location({"source_type": "map_service", "map_layer_name": "protected_areas"}) == "protected_areas"
