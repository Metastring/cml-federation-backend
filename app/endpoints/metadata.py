from fastapi import APIRouter
from app.db import get_connection

router = APIRouter()

@router.get("/metadata")
def get_metadata():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            cat.category_name AS category_name,
            ds.dataset_name AS dataset_name,
            ffm.federated_field_name,
            ffm.description,
            dffm.column_name
        FROM
            category_master cat
        JOIN
            dataset_master ds ON ds.category_id = cat.category_id
        JOIN
            federated_field_mapping dffm ON dffm.dataset_id = ds.dataset_id
        JOIN
            federated_field_master ffm ON ffm.id = dffm.federated_field_id
        ORDER BY
            cat.category_name, ds.dataset_name, ffm.federated_field_name;
    """)

    rows = cursor.fetchall()
    conn.close()

    # Convert to nested structure
    output = {}
    for row in rows:
        category_name, dataset_name, field_name, description, column_name = row

        # Add category
        if category_name not in output:
            output[category_name] = {
                "category": category_name,
                "datasets": []
            }

        # Add dataset
        datasets = output[category_name]["datasets"]
        dataset = next((d for d in datasets if d["name"] == dataset_name), None)
        if not dataset:
            dataset = {
                "name": dataset_name,
                "fields": []
            }
            datasets.append(dataset)

        # Add field
        fields = dataset["fields"]
        field = next((f for f in fields if f["field_name"] == field_name), None)
        if not field:
            field = {
                "field_name": field_name,
                "description": description,
                "mappings": []
            }
            fields.append(field)

        # Add mapping
        if column_name and column_name not in field["mappings"]:
            field["mappings"].append(column_name)

    return list(output.values())
