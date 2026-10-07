from openpyxl import Workbook


def test_is_local_file_reference_covers_bare_paths_file_uris_and_drive_letters():
    import app.dataset_registration_service as svc

    assert svc._is_local_file_reference("/data/plots.csv")
    assert svc._is_local_file_reference("file:///data/plots.csv")
    assert svc._is_local_file_reference(r"C:\data\plots.csv")
    assert not svc._is_local_file_reference("s3://bucket/plots.csv")
    assert not svc._is_local_file_reference("https://example.org/plots.csv")


def test_check_local_file_reference_reads_csv_header(tmp_path):
    import app.dataset_registration_service as svc

    path = tmp_path / "plots.csv"
    path.write_text("plot_id,species\nP1,Shorea robusta\n")

    status, detail, fields = svc._check_local_file_reference(str(path), "csv")

    assert status == "reachable"
    assert "2 columns detected" in detail
    assert fields == [
        {"field_name": "plot_id", "sample_value": "P1"},
        {"field_name": "species", "sample_value": "Shorea robusta"},
    ]


def test_check_local_file_reference_accepts_file_uri_and_infers_format_from_extension(tmp_path):
    import app.dataset_registration_service as svc

    path = tmp_path / "my plots.xlsx"
    workbook = Workbook()
    workbook.active.append(["plot_id", "dbh_cm"])
    workbook.active.append(["P1", 42])
    workbook.save(path)

    status, _, fields = svc._check_local_file_reference(path.as_uri(), None)

    assert status == "reachable"
    assert fields == [
        {"field_name": "plot_id", "sample_value": "P1"},
        {"field_name": "dbh_cm", "sample_value": "42"},
    ]


def test_check_local_file_reference_drops_row_cut_by_sniff_limit(tmp_path):
    import app.dataset_registration_service as svc

    path = tmp_path / "big.csv"
    path.write_text("a,b\n" + "x" * svc._LOCAL_SNIFF_BYTES + ",y\n")

    status, _, fields = svc._check_local_file_reference(str(path), "csv")

    assert status == "reachable"
    assert [f["field_name"] for f in fields] == ["a", "b"]


def test_check_local_file_reference_reports_missing_file_and_directory(tmp_path):
    import app.dataset_registration_service as svc

    status, detail, fields = svc._check_local_file_reference(str(tmp_path / "nope.csv"), "csv")
    assert (status, fields) == ("unreachable", None)
    assert "No such file on the backend host" in detail

    status, detail, _ = svc._check_local_file_reference(str(tmp_path), "csv")
    assert status == "unreachable"
    assert "Not a regular file" in detail
