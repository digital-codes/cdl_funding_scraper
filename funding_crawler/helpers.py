import hashlib
import json


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
    -- 1. Aggregate old data with previous update dates for each id_hash
    WITH aggregated_data_old AS (
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

    -- 2. Filter new data where on_website_to is NULL (should be one per id)
    aggregated_data_new AS (
        SELECT
            {", ".join(columns)},
            on_website_from
        FROM
            {dataset_name}
        WHERE
            on_website_to IS NULL
    )

    -- 3. Combine new data with historical updates (keep all new and merge if old exists, else null for previous dates)
    SELECT
        {", ".join(columns)},
        aggregated_data_old.previous_update_dates,
        aggregated_data_new.on_website_from AS last_updated
    FROM
        aggregated_data_new
    LEFT JOIN
        aggregated_data_old
        ON aggregated_data_new.id_hash = aggregated_data_old.agg_id
        """
    return query


def gen_license(title, scrape_date, url):
    temp = f"""
{title} von Bundesministerium für Wirtschaft und Klimaschutz, lizensiert unter CC BY-ND 3.0 DE (https://creativecommons.org/licenses/by-nd/3.0/de/deed.de), zuletzt abgerufen am {scrape_date} unter {url}
"""
    return temp
