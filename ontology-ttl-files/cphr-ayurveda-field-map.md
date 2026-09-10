# CPHR Ayurveda Ontology — Field Map

Companion to [cphr-ayurveda-ontology.ttl](./cphr-ayurveda-ontology.ttl). Maps each `cphr:` class
to the raw dataset/source columns its data actually comes from, so `GET /ontology/field-map` and
`GET /ontology/classes/{name}` (`cphr_ontology_service.py`) can show "which real field backs this
class" instead of just the abstract schema.

Grouping and table shape follow the convention `cphr_ontology_service._parse_field_map()`
expects: the 4 fixed section titles below, each with a `| Class | Covered fields |` table.
Section title → group key: Core biodiversity classes → `biodiversity`,
Traditional drug and formulation classes → `traditional_medicine`,
Curation-support classes → `curation_support`, Extension-ready classes → `extension`.

## Core biodiversity classes

| Class | Covered fields |
| --- | --- |
| `cphr:PlantSpecies` | `scientific_name`, `genus`, `specific_epithet`, `taxon_class`, `family`, `order`, `kingdom`, `vernacular_name`, `sanskrit_name`, `hindi_name`, `trade_name`, `color`, `description`, `is_traded_in_india`, `ipni_id`, `wcvp_id`, `powo_id`, `gbif_taxon_key` |
| `cphr:TaxonConcept` | `taxon_id`, `scientific_name_authorship`, `accepted_name_usage`, `nomenclatural_code` |
| `cphr:TaxonRank` | `rank_order` — controlled vocab (`cphr:Rank_*`); from `dwc:taxonRank` |
| `cphr:TaxonomicStatus` | controlled vocab (`cphr:Status_*`); from `dwc:taxonomicStatus` |
| `cphr:Concept` | `concept_title`, `concept_remarks` |
| `cphr:Habitat` | `habitat_description`, `altitude_range`, `agro_climatic_zone` |
| `cphr:ObservationMethod` | `method_name`, `method_description`, `basis_of_record` |
| `cphr:Location` | `locality`, `state_province`, `country`, `decimal_latitude`, `decimal_longitude` |
| `cphr:Document` | `document_title`, `document_type`, `bibliographic_citation`, `document_date` |
| `cphr:WebPage` | `url`, `access_date` |

Source for taxonomy / nomenclature columns: `tradedmedicinalplants.org` (Goraya & Ved 2017),
enriched from POWO / WCVP / GBIF backbone. `sanskrit_name` / `hindi_name` / `trade_name` from
AyurAhaar common-name fields.

## Traditional drug and formulation classes

| Class | Covered fields |
| --- | --- |
| `cphr:Dravya` | `dravya_name`, `sanskrit_name`, `description`, `color`, `is_traded_in_india`, `official_status`, `api_monograph_ref` |
| `cphr:Dravyaguna` | `dravyaguna_summary`, `textual_reference` |
| `cphr:Rasa` | `rasa` |
| `cphr:Guna` | `guna` |
| `cphr:Veerya` | `veerya` |
| `cphr:Vipaka` | `vipaka` |
| `cphr:Karma` | `karma`, `karma_category` |
| `cphr:Prabhava` | `prabhava_description` |
| `cphr:DoshaAction` | `dosha_action`, `action_type`, `dosha_action_note` |
| `cphr:Dosha` | `dosha_action` — controlled vocab (`cphr:Dosha_Vata/Pitta/Kapha`) |
| `cphr:Varga` | `varga`, `nighantu_source`, `varga_note` |
| `cphr:PlantPart` | `part`, `part_note` |
| `cphr:SourceType` | `source_type`, `source_type_code`, `shodhana_required` |

Source for `rasa` / `guna` / `veerya` / `vipaka` / `karma` / `dosha_action` / `varga` / `part`:
AyurAhaar `trait_detail` list, flattened by `_flatten_ayurahaar()` in `app/main.py` via
`_AYURAHAAR_TRAIT_MAP`. `source_type` / `official_status` / `api_monograph_ref` are new in v0.2 —
not yet backed by a live column (curation-entered until an API/Nighantu source is wired in).

## Curation-support classes

| Class | Covered fields |
| --- | --- |
| `cphr:AyurvedaEntity` | _(none — organisational root)_ |

## Extension-ready classes

| Class | Covered fields |
| --- | --- |
| `cphr:Disease` | `disease_name`, `icd_code`, `namaste_code` — no source table yet |
| `cphr:Nidana` | `nidana_description` — no source table yet |
