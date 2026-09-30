"""Generate db_migrations/20260930b_dataset_mapping_field_metadata_seed.sql.

Backfills sample_value / value_range / ontology_uri on the dataset_mapping rows
that predate those columns:

- ontology_uri: ontology_mapping itself when it is already an IRI (or an obo:
  CURIE); else the primary match of the cml_term the field resolves to; else
  the Darwin Core / Dublin Core / CPHR Ayurveda IRI in _AYURVEDA_URIS (the
  Ayurveda ontology was dropped from Fuseki on 2026-09-25, but its IRIs and
  their dwc/dcterms equivalents are still the right identifiers).
- metadata: the ontology label/graph, the cml_term's definition and unit
  when the field resolves to one, and where the sample came from.
- sample_value / value_range: from the dataset's own table in this DB (via
  map_layer_info) when the column exists, otherwise from a live
  /federated-search against the backend for the external participant APIs.

The SQL is keyed on (dataset title, field_name) rather than ids so the same
file applies to any peer's DB, and only fills columns that are still NULL.

Run from the backend root with the deployed .env loaded:
    env/bin/python scripts/build_dataset_mapping_metadata_seed.py [--api http://localhost:8000]
"""
from __future__ import annotations

import argparse
import datetime
import decimal
import json
import re
import sys
from collections import Counter
from pathlib import Path

import requests
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.db import get_connection  # noqa: E402
from app.endpoints.categories_with_datasets import _load_term_crosswalk, _resolve_term  # noqa: E402

OUT = Path(__file__).resolve().parent.parent / "db_migrations" / "20260930b_dataset_mapping_field_metadata_seed.sql"

DWC = "http://rs.tdwg.org/dwc/terms/"
DCT = "http://purl.org/dc/terms/"
CPHR = "http://cml.org/ontology/ayurveda#"

# ontology_mapping value (the old ayurveda field names) -> IRI. The standard
# dwc/dcterms term wherever the Ayurveda TTL declared one equivalent.
_AYURVEDA_URIS = {
    "plant_species": DWC + "scientificName",
    "vernacular_name_common_names": DWC + "vernacularName",
    "english_name": DWC + "vernacularName",
    "genus": DWC + "genus",
    "specific_epithet": DWC + "specificEpithet",
    "species": DWC + "specificEpithet",
    "family": DWC + "family",
    "order": DWC + "order",
    "kingdom": DWC + "kingdom",
    "author": DWC + "scientificNameAuthorship",
    "continent": DWC + "continent",
    "region": DWC + "locality",
    "area": DWC + "locality",
    "remarks": DWC + "occurrenceRemarks",
    "event_date": DWC + "eventDate",
    "country_code": DWC + "countryCode",
    "basis_of_record": DWC + "basisOfRecord",
    "description": DCT + "description",
    "refstand": DCT + "bibliographicCitation",
    "trade_name": CPHR + "tradeName",
    "official_name": CPHR + "dravyaName",
    "drug_name": CPHR + "dravyaName",
    "ingredient_name": CPHR + "dravyaName",
    "sanskrit_name": CPHR + "sanskritName",
    "hindi_name": CPHR + "hindiName",
    "color": CPHR + "color",
    "is_traded_in_india": CPHR + "isTradedInIndia",
    "ipni_id": CPHR + "ipniId",
    "wcvp_id": CPHR + "wcvpId",
    "disease": CPHR + "diseaseName",
    "doshas": CPHR + "involvesDosha",
    "dravyaguna": CPHR + "dravyagunaSummary",
    "rasa": CPHR + "hasRasa",
    "guna": CPHR + "hasGuna",
    "veerya": CPHR + "hasVeerya",
    "vipaka": CPHR + "hasVipaka",
    "karma": CPHR + "hasKarma",
    "dosha_action": CPHR + "hasDoshaAction",
    "varga": CPHR + "belongsToVarga",
    "plant_part": CPHR + "hasUsedPart",
    "recipe": CPHR + "Dravya",
}

_FREE_TEXT = "Free text"
_RASA = "Madhura (Sweet); Amla (Sour); Lavana (Salty); Katu (Pungent); Tikta (Bitter); Kashaya (Astringent)"
_GUNA = ("Guru (Heavy); Laghu (Light); Manda (Slow); Tiksna (Sharp); Hima (Cold); Usna (Hot); "
         "Snigdha (Unctuous); Ruksa (Dry); Slaksna (Smooth); Khara (Rough); Sandra (Dense); Drava (Liquid); "
         "Mrdu (Soft); Kathina (Hard); Sthira (Stable); Sara (Mobile); Suksma (Subtle); Sthula (Gross); "
         "Visada (Clear); Picchila (Slimy)")

# Fallback (sample_value, value_range) by ontology_mapping, used only where
# the source gave nothing -- external APIs that don't return the field, or a
# field with no source at all. Ranges for the Ayurveda property fields are
# their classical controlled vocabularies; the rest describe the value's
# form. Rows that use these get metadata.curated listing which columns.
_CURATED = {
    "plant_species": ("Withania somnifera (L.) Dunal; Ocimum tenuiflorum L.; Azadirachta indica A.Juss.",
                      "Binomial scientific name with author citation"),
    "vernacular_name_common_names": ("Ashwagandha; Tulsi; Neem",
                                     "Free text; several names per plant across Indian languages"),
    "english_name": ("Indian gooseberry; Winter cherry; Three myrobalans", "Free text (English name)"),
    "trade_name": ("Ashwagandha; Amla; Shatavari", "Free text (name used in the herbal raw-drug trade)"),
    "official_name": ("Ashvagandha; Amalaki; Guduchi",
                      "Official drug name per the Ayurvedic Pharmacopoeia of India"),
    "drug_name": (None, "Classical Sanskrit drug name (free text)"),
    "ingredient_name": (None, "Free text (food ingredient name, with form/process)"),
    "sanskrit_name": (None, "Free text (IAST transliteration or Devanagari)"),
    "hindi_name": ("Adrak; Haldi; Chawal", "Free text (Hindi name)"),
    "disease": ("Jvara (fever); Pandu (anaemia); Kushtha (skin disease)", "Free text (classical disease name)"),
    "doshas": ("Vata; Pitta; Kapha", "Vata; Pitta; Kapha"),
    "recipe": ("Khichdi; Takra; Yavagu", "Free text (recipe name)"),
    "genus": ("Withania; Zingiber; Curcuma", "Botanical genus"),
    "specific_epithet": ("somnifera; officinale; longa", "Botanical specific epithet"),
    "species": (None, "Botanical specific epithet"),
    "family": ("Solanaceae; Zingiberaceae; Fabaceae", "Botanical family"),
    "order": ("Solanales; Zingiberales; Fabales", "Botanical order"),
    "kingdom": ("Plantae", "Plantae; Fungi; Animalia"),
    "author": (None, "Botanical author abbreviation (IPNI standard form)"),
    "color": (None, "Free text (e.g. Red; White; Black; Yellow; Green)"),
    "description": ("Rhizome of Zingiber officinale, used fresh or dried as a spice", _FREE_TEXT),
    "is_traded_in_india": ("true; false", "true; false"),
    "ipni_id": ("797962-1; 30000181-2", "IPNI plant name ID (<digits>-<version>)"),
    "wcvp_id": ("461906; 2604", "WCVP plant_name_id (integer)"),
    "dravyaguna": ("Deepana; Pachana; Vatanulomana", "Free text (classical pharmacological summary)"),
    "rasa": (None, _RASA),
    "guna": (None, _GUNA),
    "veerya": (None, "Usna (Hot); Sita (Cold)"),
    "vipaka": (None, "Madhura (Sweet); Amla (Sour); Katu (Pungent)"),
    "dosha_action": (None, "Vatahara; Pittahara; Kaphahara; Vatakara; Pittakara; Kaphakara; Tridoshahara"),
    "karma": ("Deepana; Pachana; Balya", "Free text (classical pharmacological action)"),
    "varga": (None, "Nighantu varga name (e.g. Dhanya varga; Shaka varga; Phala varga)"),
    "plant_part": ("Rhizome; Seed; Leaf", "Root; Rhizome; Stem; Bark; Leaf; Flower; Fruit; Seed; Whole plant"),
    "remarks": (None, _FREE_TEXT),
    "obo:RO_0000053": ("http://purl.obolibrary.org/obo/PATO_0000014",
                       "IRI of a characteristic (quality) term"),
}

# ontology_mapping -> IRI for fields neither the crosswalk nor the Ayurveda
# map covers.
_EXTRA_URIS = {
    "indicator_category": "http://purl.org/linked-data/cube#DimensionProperty",
}

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}")

# Search texts that return a representative handful of rows from each
# external participant API (they have no "list all" call).
_API_PROBES = {
    "Traded Medicinal Plants of India (TMPI)": ["ashwagandha", "tulsi", "neem"],
    "Rasashastra: A Database of Metals and Minerals used in Ayurveda": ["gold", "iron", "mercury"],
    "Ayurahaar – The Ahara & Nutrition Portal": ["ginger", "rice", "turmeric"],
    "CPMP Drug Source": ["ashwagandha", "amalaki", "triphala"],
    "CPMP Botanical Source": ["withania", "ocimum", "azadirachta"],
}

SAMPLE_COUNT = 3
# Values often contain commas themselves ("Ginger, fresh"), so join with ";".
SEP = "; "
MAX_SAMPLE_LEN = 60
MAX_CATEGORICAL = 12
_NUMERIC_OR_DATE = {"smallint", "integer", "bigint", "numeric", "real", "double precision", "date",
                    "timestamp without time zone", "timestamp with time zone"}


def _norm(name: str) -> str:
    return re.sub(r"[\s_\-]", "", name or "").lower()


def _fmt(value) -> str:
    if isinstance(value, float):
        return f"{value:g}"
    if isinstance(value, decimal.Decimal):
        return f"{value.normalize():f}"
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text if len(text) <= MAX_SAMPLE_LEN else text[: MAX_SAMPLE_LEN - 1] + "…"


def _summarise(values: list) -> tuple[str | None, str | None]:
    """(sample_value, value_range) from raw values pulled off an API."""
    flat = []
    for v in values:
        flat.extend(v if isinstance(v, list) else [v])
    flat = [v for v in flat if v not in (None, "") and str(v).lower() != "nan" and not isinstance(v, dict)]
    if not flat:
        return None, None
    counts = Counter(_fmt(v) for v in flat)
    sample = SEP.join(v for v, _ in counts.most_common(SAMPLE_COUNT))
    if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in flat):
        return sample, f"{_fmt(min(flat))} – {_fmt(max(flat))}"
    if all(isinstance(v, bool) for v in flat):
        return sample, "true, false"
    return sample, None


def _ontology_uri(row: dict, term: str | None, crosswalk) -> str | None:
    mapping = (row["ontology_mapping"] or "").strip()
    if mapping.startswith(("http://", "https://")):
        return mapping
    if mapping.startswith("obo:"):
        return "http://purl.obolibrary.org/obo/" + mapping[4:]
    refs = crosswalk[3].get(term, []) if term else []
    if refs and refs[0]["uri"]:
        return refs[0]["uri"]
    return _AYURVEDA_URIS.get(mapping) or _EXTRA_URIS.get(mapping)


def _table_columns(cur, table: str) -> dict[str, tuple[str, str]]:
    cur.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = %s",
        (table,),
    )
    return {_norm(r["column_name"]): (r["column_name"], r["data_type"]) for r in cur.fetchall()}


def _from_table(cur, table: str, column: str, data_type: str) -> tuple[str | None, str | None]:
    tbl, col = sql.Identifier(table), sql.Identifier(column)
    cur.execute(
        sql.SQL(
            "SELECT {c} AS v FROM {t} WHERE {c} IS NOT NULL AND lower({c}::text) NOT IN ('', 'nan') "
            "GROUP BY {c} ORDER BY COUNT(*) DESC, {c} LIMIT %s"
        ).format(c=col, t=tbl),
        (SAMPLE_COUNT,),
    )
    samples = [_fmt(r["v"]) for r in cur.fetchall()]
    if not samples:
        return None, None
    if data_type in _NUMERIC_OR_DATE:
        cur.execute(sql.SQL("SELECT MIN({c}) AS lo, MAX({c}) AS hi FROM {t}").format(c=col, t=tbl))
        r = cur.fetchone()
        return SEP.join(samples), f"{_fmt(r['lo'])} – {_fmt(r['hi'])}"
    cur.execute(
        sql.SQL("SELECT DISTINCT {c} AS v FROM {t} WHERE {c} IS NOT NULL AND lower({c}::text) NOT IN ('', 'nan') ORDER BY 1 LIMIT %s")
        .format(c=col, t=tbl),
        (MAX_CATEGORICAL + 1,),
    )
    distinct = [_fmt(r["v"]) for r in cur.fetchall()]
    if len(distinct) <= MAX_CATEGORICAL:
        return SEP.join(samples), SEP.join(distinct)
    if all(_ISO_DATE.match(v) for v in samples):
        cur.execute(
            sql.SQL("SELECT MIN({c}::text) AS lo, MAX({c}::text) AS hi FROM {t} WHERE {c}::text ~ '^[0-9]{{4}}-'")
            .format(c=col, t=tbl)
        )
        r = cur.fetchone()
        return SEP.join(samples), f"{r['lo'][:10]} – {r['hi'][:10]}"
    cur.execute(sql.SQL("SELECT COUNT(DISTINCT {c}) AS n FROM {t}").format(c=col, t=tbl))
    return SEP.join(samples), f"{cur.fetchone()['n']:,} distinct values"


def _api_rows(api: str, title: str) -> list[dict]:
    rows = []
    for text in _API_PROBES.get(title, []):
        try:
            resp = requests.post(
                f"{api}/federated-search",
                json={"category": ["Health"], "dataset": [title], "fields": [], "search_text": text, "scope": "local"},
                timeout=90,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            print(f"  ! {title} / {text!r}: {exc}", file=sys.stderr)
            continue
        for ds in (resp.json().get("results") or {}).values():
            for group in (ds.get("field_results") or {}).values():
                rows.extend(r for r in group.get("results") or [] if isinstance(r, dict))
    return rows


def _metadata(row: dict, term: dict | None, source: str | None) -> dict | None:
    meta = {
        "label": row["ontology_mapping_to_display"],
        "ontology": row["ontology_graph_key"],
        "cml_term": term and term["term_key"],
        "definition": term and term["definition"],
        "unit": term and term["unit_uri"],
        "source": source,
    }
    meta = {k: v for k, v in meta.items() if v}
    return meta or None


def _literal(value: str | None) -> str:
    return "NULL" if value is None else "'" + value.replace("'", "''") + "'"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--api", default="http://localhost:8000", help="backend used for external participant APIs")
    args = parser.parse_args()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    crosswalk = _load_term_crosswalk(cur)
    cur.execute("SELECT term_key, definition, unit_uri FROM cml_term")
    terms = {r["term_key"]: r for r in cur.fetchall()}
    cur.execute(
        """
        SELECT dm.dataset_mapping_id, dm.field_name, dm.ontology_mapping,
               dm.ontology_mapping_to_display, dm.ontology_graph_key, d.title,
               (SELECT split_part(m.geoserver_name, ':', 2) FROM map_layer_info m
                WHERE m.dataset_id = d.dataset_id ORDER BY m.created_at DESC LIMIT 1) AS data_table
        FROM dataset_mapping dm
        JOIN dataset_master d ON d.dataset_id = dm.dataset_id
        WHERE d.title IS NOT NULL
        ORDER BY d.title, dm.dataset_mapping_id
        """
    )
    mappings = cur.fetchall()

    api_cache: dict[str, list[dict]] = {}
    statements = []
    for row in mappings:
        term = _resolve_term(row["field_name"], row["ontology_mapping"], crosswalk)
        uri = _ontology_uri(row, term, crosswalk)
        sample = value_range = source = None

        columns = _table_columns(cur, row["data_table"]) if row["data_table"] else {}
        column = columns.get(_norm(row["field_name"]))
        if column:
            sample, value_range = _from_table(cur, row["data_table"], *column)
            source = f"table {row['data_table']}.{column[0]} ({column[1]})"
        elif row["title"] in _API_PROBES:
            if row["title"] not in api_cache:
                api_cache[row["title"]] = _api_rows(args.api, row["title"])
            keys = {_norm(row["field_name"]), _norm(row["ontology_mapping"])}
            values = [v for r in api_cache[row["title"]] for k, v in r.items() if _norm(k) in keys]
            sample, value_range = _summarise(values)
            source = "external API" if values else None
        curated = []
        fallback_sample, fallback_range = _CURATED.get(row["ontology_mapping"], (None, None))
        if not sample and fallback_sample:
            sample = fallback_sample
            curated.append("sample_value")
        if not value_range and (fallback_range or sample):
            value_range = fallback_range or _FREE_TEXT
            curated.append("value_range")
        metadata = _metadata(row, terms.get(term), source)
        if curated:
            metadata = {**(metadata or {}), "curated": curated}

        print(f"{row['title'][:40]:40} {row['field_name'][:24]:24} uri={uri or '-'} sample={sample or '-'} range={value_range or '-'}")
        if not (uri or sample or value_range or metadata):
            continue
        meta_sql = "'{}'::jsonb" if metadata is None else _literal(json.dumps(metadata, ensure_ascii=False)) + "::jsonb"
        statements.append(
            "UPDATE dataset_mapping dm SET\n"
            f"    sample_value = COALESCE(dm.sample_value, {_literal(sample)}),\n"
            f"    value_range = COALESCE(dm.value_range, {_literal(value_range)}),\n"
            f"    ontology_uri = COALESCE(dm.ontology_uri, {_literal(uri)}),\n"
            f"    metadata = {meta_sql} || COALESCE(dm.metadata, '{{}}'::jsonb)\n"
            f"  FROM dataset_master d WHERE d.dataset_id = dm.dataset_id\n"
            f"    AND d.title = {_literal(row['title'])} AND dm.field_name = {_literal(row['field_name'])};"
        )
    conn.close()

    OUT.write_text(
        "-- GENERATED by scripts/build_dataset_mapping_metadata_seed.py from the rows and\n"
        "-- source data present when it was run -- regenerate rather than hand-edit.\n"
        "-- Keyed on (dataset title, field_name); only fills NULL columns and only adds\n"
        "-- missing metadata keys, so it is safe to re-run and to apply on any peer's DB.\n"
        "-- metadata.curated lists columns filled from the script's curated fallbacks\n"
        "-- rather than read from the source. Requires 20260930.\n\n"
        "BEGIN;\n\n" + "\n\n".join(statements) + "\n\nCOMMIT;\n"
    )
    print(f"wrote {len(statements)} updates to {OUT}")


if __name__ == "__main__":
    main()
