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


def gen_query(dataset_name, columns):
    query = f"""
    -- 1. Aggregate retired data with previous update dates for each id_hash
    WITH aggregated_data_retired AS (
        SELECT
            id_hash AS agg_id,
            ARRAY_AGG(on_website_to) AS previous_update_dates
        FROM
            {dataset_name}
        WHERE
            on_website_to IS NOT NULL
        GROUP BY
            id_hash
    ),

    -- 2. Filter new or unchanged data where on_website_to is NULL (should be one per id)
    data_new AS (
        SELECT
            id_hash AS new_id_hash,
            {", ".join([col for col in columns if col != "id_hash"])},
            on_website_from
        FROM
            {dataset_name}
        WHERE
            on_website_to IS NULL
    )

    -- 3. Combine new data with historical updates (include all old data as well)
    SELECT
        COALESCE(data_new.new_id_hash, aggregated_data_retired.agg_id) AS id_hash,
        {", ".join([f"data_new.{col}" for col in columns if col != "id_hash"])},
        aggregated_data_retired.previous_update_dates,
        data_new.on_website_from AS last_updated,
        CASE 
            WHEN aggregated_data_retired.agg_id IS NOT NULL AND data_new.new_id_hash IS NULL THEN TRUE
            ELSE FALSE
        END AS deleted
    FROM
        data_new
    FULL OUTER JOIN
        aggregated_data_retired
        ON data_new.new_id_hash = aggregated_data_retired.agg_id
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
        # Get the base type (handling Optional/List wrappers)
        base_type = field
        if hasattr(field, "__origin__"):
            if field.__origin__ is Union:  # handles Optional
                base_type = field.__args__[0]
            elif field.__origin__ is list:  # handles List
                continue  # Let Polars handle list types automatically

        # Map Python/Pydantic types to Polars types
        if base_type is str:
            schema_overrides[field_name] = pl.Utf8

    return schema_overrides
