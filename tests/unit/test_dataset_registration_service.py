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
