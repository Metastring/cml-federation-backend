from fastapi import APIRouter, HTTPException
from app.db import get_connection
from psycopg2.extras import RealDictCursor

router = APIRouter()



@router.get("/metadata")
def get_metadata(title: str, category_name: str):
    # Normalize title
    normalized_title = title
    
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT
                cat.category_name,
                ds.title AS dataset_title,
                ds.description,
                ds.citation,
                ds.doi,
                ds.language,
                ds.data_language,
                ds.license,
                ds.publication_date,
                ds.metadata_modified_date AS last_updated,
                ds.registration_date,
                ds.is_active,
                ds.keywords,
                ds.dataset_type,

                c.name AS contact_name,
                c.role AS contact_role,
                c.email AS contact_email,
                c.organization AS contact_organization,
                c.address AS contact_address,
                c.city AS contact_city,
                c.state AS contact_state,
                c.country AS contact_country,

                p.publisher_name,
                p.country AS publisher_country,
                p.record_count,

                s.temporal_start_date,
                s.temporal_end_date,
                s.geographic_scope,
                s.taxonomic_scope,
                s.taxonomic_authority,

                m.field_name,
                m.ontology_mapping,
                m.data_type,

                st.stat_name,
                st.stat_value,
                st.measurement_date
                
            FROM
                category_master cat
            JOIN
                dataset_master ds ON ds.category_id = cat.category_id
            LEFT JOIN
                dataset_contacts c ON c.dataset_id = ds.dataset_id
            LEFT JOIN
                dataset_publisher p ON p.dataset_id = ds.dataset_id
            LEFT JOIN
                dataset_scope s ON s.dataset_id = ds.dataset_id
            LEFT JOIN
                dataset_mapping m ON m.dataset_id = ds.dataset_id
            LEFT JOIN
                dataset_statistics st ON st.dataset_id = ds.dataset_id

            WHERE
                cat.category_name = %s AND ds.title = %s

            ORDER BY
                cat.category_name, ds.title, m.field_name;
        """, (category_name, normalized_title))

        rows = cursor.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail="Metadata not found for the specified dataset and category")

        dataset_details = {
            "category_name": category_name,
            "dataset_title": normalized_title,  # return alias instead of original
            "description": rows[0]['description'],
            "citation": rows[0]['citation'],
            "doi": rows[0]['doi'],
            "language": rows[0]['language'],
            "data_language": rows[0]['data_language'],
            "license": rows[0]['license'],
            "publication_date": rows[0]['publication_date'],
            "last_updated": rows[0]['last_updated'],
            "registration_date": rows[0]['registration_date'],
            "is_active": rows[0]['is_active'],
            "keywords": rows[0]['keywords'],
            "dataset_type": rows[0]['dataset_type'],
            "contacts": [],
            "publishers": [],
            "scopes": [],
            "fields": [],
            "statistics": []
        }

        for row in rows:
            contact = {
                "name": row['contact_name'],
                "role": row['contact_role'],
                "email": row['contact_email'],
                "organization": row['contact_organization'],
                "address": row['contact_address'],
                "city": row['contact_city'],
                "state": row['contact_state'],
                "country": row['contact_country']
            }
            if contact and contact not in dataset_details["contacts"]:
                dataset_details["contacts"].append(contact)

            publisher = {
                "publisher_name": row['publisher_name'],
                "country": row['publisher_country'],
                "record_count": row['record_count']
            }
            if publisher and publisher not in dataset_details["publishers"]:
                dataset_details["publishers"].append(publisher)

            scope = {
                "temporal_start_date": row['temporal_start_date'],
                "temporal_end_date": row['temporal_end_date'],
                "geographic_scope": row['geographic_scope'],
                "taxonomic_scope": row['taxonomic_scope'],
                "taxonomic_authority": row['taxonomic_authority']
            }
            if scope not in dataset_details["scopes"]:
                dataset_details["scopes"].append(scope)

            field = {
                "field_name": row['field_name'],
                "ontology_mapping": row['ontology_mapping'],
                "data_type": row['data_type']
            }
            if field not in dataset_details["fields"]:
                dataset_details["fields"].append(field)

            stat = {
                "stat_name": row['stat_name'],
                "stat_value": row['stat_value'],
                "measurement_date": row['measurement_date']
            }
            if stat not in dataset_details["statistics"]:
                dataset_details["statistics"].append(stat)

        return dataset_details
    finally:
        conn.close()
