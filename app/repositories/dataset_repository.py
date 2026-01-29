from app.core.database import get_connection
from app.schemas.dataset_schema import DatasetDetailsInput, DatasetRegistryInput, DatasetMappingUpdateInput
from typing import Any, Dict

class DatasetRepository:
    @staticmethod
    def save_dataset_details(details: DatasetDetailsInput) -> Dict[str, Any]:
        try:
            conn = get_connection()
            cur = conn.cursor()

            # dataset_scope
            if details.scopes:
                for s in details.scopes:
                    cur.execute("""
                        INSERT INTO dataset_scope (
                            dataset_id, temporal_start_date, temporal_end_date,
                            geographic_scope, taxonomic_scope, taxonomic_authority
                        ) VALUES (%s,%s,%s,%s,%s,%s)
                    """, (
                        details.dataset_id, s.temporal_start_date, s.temporal_end_date,
                        s.geographic_scope, s.taxonomic_scope, s.taxonomic_authority
                    ))

            # dataset_publisher
            if details.publishers:
                for p in details.publishers:
                    cur.execute("""
                        INSERT INTO dataset_publisher (
                            dataset_id, publisher_name, record_count
                        ) VALUES (%s,%s,%s)
                    """, (details.dataset_id, p.publisher_name, p.record_count))

            # dataset_contacts
            if details.contacts:
                for c in details.contacts:
                    cur.execute("""
                        INSERT INTO dataset_contacts (
                            dataset_id, name, role, email, organization, address,
                            city, state, country
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        details.dataset_id, c.name, c.role, c.email, c.organization,
                        c.address, c.city, c.state, c.country
                    ))

            # dataset_mapping
            if details.mappings:
                for m in details.mappings:
                    cur.execute("""
                        INSERT INTO dataset_mapping (
                            dataset_id, field_name, ontology_mapping, data_type
                        ) VALUES (%s,%s,%s,%s)
                    """, (details.dataset_id, m.field_name, m.ontology_mapping, m.data_type))

            # dataset_metrics
            if details.metrics:
                for m in details.metrics:
                    cur.execute("""
                        INSERT INTO dataset_metrics (
                            dataset_id, metric_name, metric_value
                        ) VALUES (%s,%s,%s)
                    """, (details.dataset_id, m.metric_name, m.metric_value))

            # dataset_statistics
            if details.statistics:
                for s in details.statistics:
                    cur.execute("""
                        INSERT INTO dataset_statistics (
                            dataset_id, stat_name, stat_value, measurement_date
                        ) VALUES (%s,%s,%s,%s)
                    """, (details.dataset_id, s.stat_name, s.stat_value, s.measurement_date))

            conn.commit()
            cur.close()
            conn.close()

            return {"status": "success", "message": "All dataset details saved"}

        except Exception as e:
            return {"status": "error", "error": str(e)}

    @staticmethod
    def create_dataset_registry(payload: DatasetRegistryInput) -> Dict[str, Any]:
        try:
            conn = get_connection()
            cur = conn.cursor()

            # Handle category: prefer existing by id, then by name; insert only if name not found
            category_id = None
            if payload.category:
                if payload.category.category_id:
                    cur.execute("SELECT category_id FROM category_master WHERE category_id = %s", (payload.category.category_id,))
                    row = cur.fetchone()
                    if row:
                        category_id = row[0]
                    else:
                        return {"status": "error", "error": f"category_id {payload.category.category_id} does not exist"}
                elif payload.category.category_name:
                    cur.execute("SELECT category_id FROM category_master WHERE category_name = %s", (payload.category.category_name,))
                    row = cur.fetchone()
                    if row:
                        category_id = row[0]
                    else:
                        cur.execute("""
                            INSERT INTO category_master (category_name)
                            VALUES (%s)
                            RETURNING category_id
                        """, (payload.category.category_name,))
                        category_id = cur.fetchone()[0]

            from datetime import datetime
            system_date = datetime.now().date()
            cur.execute("""
                INSERT INTO dataset_master (
                    title, description, citation, doi, language,
                    data_language, license, publication_date,
                    metadata_modified_date, registration_date, is_active,
                    keywords, dataset_type, category_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING dataset_id;
            """, (
                payload.title, payload.description, payload.citation,
                payload.doi, payload.language, payload.data_language,
                payload.license, system_date, system_date,
                system_date, payload.is_active, payload.keywords,
                payload.dataset_type, category_id
            ))
            dataset_id = cur.fetchone()[0]

            # dataset_publisher (avoid duplicates by name+dataset_id)
            if payload.publishers:
                for p in payload.publishers:
                    cur.execute("SELECT 1 FROM dataset_publisher WHERE dataset_id=%s AND publisher_name=%s", (dataset_id, p.publisher_name))
                    if not cur.fetchone():
                        cur.execute("""
                            INSERT INTO dataset_publisher (
                                dataset_id, publisher_name, record_count
                            ) VALUES (%s,%s,%s)
                        """, (dataset_id, p.publisher_name, p.record_count))

            # dataset_contacts (avoid duplicates by name+email+dataset_id)
            if payload.contacts:
                for c in payload.contacts:
                    cur.execute("SELECT 1 FROM dataset_contacts WHERE dataset_id=%s AND name=%s AND email=%s", (dataset_id, c.name, c.email))
                    if not cur.fetchone():
                        cur.execute("""
                            INSERT INTO dataset_contacts (
                                dataset_id, name, role, email, organization, address,
                                city, state, country
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """, (
                            dataset_id, c.name, c.role, c.email, c.organization,
                            c.address, c.city, c.state, c.country
                        ))

            # source_master (avoid duplicates by source_name+dataset_id)
            if payload.sources:
                for s in payload.sources:
                    cur.execute("SELECT 1 FROM source_master WHERE source_name=%s AND dataset_id=%s", (s.source_name, dataset_id))
                    if not cur.fetchone():
                        cur.execute("""
                            INSERT INTO source_master (
                                source_name, dataset_id, base_url, description
                            ) VALUES (%s,%s,%s,%s)
                        """, (s.source_name, dataset_id, s.base_url, s.description))

            # dataset_statistics (avoid duplicates by stat_name+dataset_id+measurement_date)
            if payload.statistics:
                for st in payload.statistics:
                    cur.execute("SELECT 1 FROM dataset_statistics WHERE dataset_id=%s AND stat_name=%s AND measurement_date=%s", (dataset_id, st.stat_name, st.measurement_date))
                    if not cur.fetchone():
                        cur.execute("""
                            INSERT INTO dataset_statistics (
                                dataset_id, stat_name, stat_value, measurement_date
                            ) VALUES (%s,%s,%s,%s)
                        """, (dataset_id, st.stat_name, st.stat_value, st.measurement_date))

            conn.commit()
            cur.close()
            conn.close()

            return {"status": "success", "dataset_id": dataset_id, "category_id": category_id}

        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            return {"status": "error", "error": str(e)}
