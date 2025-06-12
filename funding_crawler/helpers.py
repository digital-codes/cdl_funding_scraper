import hashlib
import json
from pydantic import BaseModel
import polars as pl
from typing import Union, Dict, Any


def compute_checksum(data: dict, fields: list[str]) -> str:
    """
    compute a checksum for specified fields in a dictionary
    """
    selected_data = {key: data[key] for key in sorted(fields) if key in data}

    serialized_data = json.dumps(selected_data, separators=(",", ":"), sort_keys=True)

    checksum = hashlib.sha256(serialized_data.encode()).hexdigest()

    return checksum


def gen_comp_b(dataset_name, columns):
    # Filter new or unchanged data where on_website_to is NULL (should be one per id)
    return f"""
        SELECT
            id_hash AS new_id_hash,
            {", ".join([col for col in columns if col != "id_hash"])},
            on_website_from
        FROM
            {dataset_name}
        WHERE
            on_website_to IS NULL"""


def gen_comp_a(dataset_name):
    # Aggregate retired data with previous update dates for each id_hash
    return f"""
SELECT
            id_hash AS agg_id,
            ARRAY_AGG(on_website_to ORDER BY on_website_to) AS previous_update_dates,
            MAX(on_website_to) as last_updated
        FROM
            {dataset_name}
        WHERE
            on_website_to IS NOT NULL
        GROUP BY
            id_hash
            """


def gen_comp_c(dataset_name, columns):
    return f"""
     WITH aggregated_data_retired AS(
        {gen_comp_a(dataset_name)}
     )
     SELECT 
            aggregated_data_retired.agg_id,
            aggregated_data_retired.previous_update_dates,
            {dataset_name}.on_website_from,
            aggregated_data_retired.last_updated,
            {", ".join([f"{dataset_name}.{col}" for col in columns if col != "id_hash"])}
        FROM aggregated_data_retired
        JOIN {dataset_name} ON aggregated_data_retired.agg_id = {dataset_name}.id_hash 
            AND aggregated_data_retired.last_updated = {dataset_name}.on_website_to"""


def gen_query(dataset_name, columns):
    coalesce_columns = [
        f"COALESCE(data_new.{col}, most_recent_data_retired.{col}) AS {col}"
        for col in columns
        if col != "id_hash"
    ]

    query = f"""
    WITH data_new AS (
        {gen_comp_b(dataset_name, columns)}
    ),
    most_recent_data_retired AS (
        {gen_comp_c(dataset_name, columns)}
    ),
    deleted_records AS (
        SELECT 
            most_recent_data_retired.agg_id
        FROM 
            most_recent_data_retired
        LEFT JOIN 
            data_new
            ON most_recent_data_retired.agg_id = data_new.new_id_hash
        WHERE 
            data_new.new_id_hash IS NULL
    )

    SELECT
        COALESCE(data_new.new_id_hash, most_recent_data_retired.agg_id) AS id_hash,
        {", ".join(coalesce_columns)},
        most_recent_data_retired.previous_update_dates AS previous_update_dates,
        most_recent_data_retired.last_updated AS last_updated,
        COALESCE(data_new.on_website_from, most_recent_data_retired.on_website_from) AS on_website_from,
        CASE 
            WHEN deleted_records.agg_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS deleted
        FROM
            data_new
        FULL OUTER JOIN
            most_recent_data_retired
            ON data_new.new_id_hash = most_recent_data_retired.agg_id
        LEFT JOIN
            deleted_records
            ON COALESCE(data_new.new_id_hash, most_recent_data_retired.agg_id) = deleted_records.agg_id
    """
    return query


def gen_license(title, scrape_date, url):
    temp = f"""
{title} von Bundesministerium für Wirtschaft und Klimaschutz, lizensiert unter CC BY-ND 3.0 DE (https://creativecommons.org/licenses/by-nd/3.0/de/deed.de), zuletzt abgerufen am {scrape_date} unter {url}
"""
    return temp


def pydantic_to_polars_schema(model: type[BaseModel]) -> Dict[str, Any]:
    """Convert Pydantic model fields to Polars schema overrides."""
    schema_overrides = {}
    for field_name, field in model.__annotations__.items():
        base_type = field
        if hasattr(field, "__origin__"):
            if field.__origin__ is Union:
                base_type = field.__args__[0]
            elif field.__origin__ is list:
                continue

        if base_type is str:
            schema_overrides[field_name] = pl.Utf8

    return schema_overrides
