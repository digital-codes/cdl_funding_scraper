import dlt
from funding_crawler.scrapy_utils import FundingSpider
from funding_crawler.dlt_utils.helpers import create_pipeline_runner, cfg_provider
from scrapy_settings import scrapy_settings
from funding_crawler.models import FundingProgramSchema
import polars as pl
import boto3
import zipfile
from sqlalchemy import create_engine
import os
import modal

license_content = """
This data was scraped from the website: foerderdatenbank.de. 
The content is licensed under the Creative Commons Attribution-NoDerivatives 3.0 Germany License (CC BY-ND 3.0 DE) (https://creativecommons.org/licenses/by-nd/3.0/de/). 
© 2024 www.bmwk.de
"""

image = (
    modal.Image.debian_slim()
    .copy_local_file("requirements.txt")
    .run_commands("pip install uv && uv pip install --system -r requirements.txt")
)

app = modal.App(name="cdl_awo_funding_crawler", image=image)


@app.function(
    mounts=[
        modal.Mount.from_local_python_packages("funding_crawler"),
        modal.Mount.from_local_file(
            local_path="dlt_config.toml", remote_path="/root/dlt_config.toml"
        ),
        modal.Mount.from_local_file(
            local_path="scrapy_settings.py", remote_path="/root/scrapy_settings.py"
        ),
    ],
    secrets=[
        modal.Secret.from_local_environ(
            [
                "POSTGRES_CONN_STR",
                "DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL",
                "DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID",
                "DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY",
            ]
        ),
    ],
    schedule=modal.Cron("0 2 */2 * *"),
    timeout=3600,
)
def crawl():
    dataset_name = "foerderdatenbankdumpbackend"
    bucket_name = "foerderdatenbankdump"

    local_license_file_name = "LICENSE"
    local_zip_file_name = "data.zip"
    local_data_file_name = "db.parquet"
    remote_file_name = f"data/{local_zip_file_name}"

    postgres_conn_str = os.getenv("POSTGRES_CONN_STR")
    assert postgres_conn_str

    engine = create_engine(postgres_conn_str)

    dlt.config.register_provider(cfg_provider)

    pipeline = dlt.pipeline(
        pipeline_name="funding_crawler",
        destination=dlt.destinations.postgres(postgres_conn_str),
        dataset_name=dataset_name,
    )

    scraping_host = create_pipeline_runner(
        pipeline, FundingSpider, batch_size=50, scrapy_settings=scrapy_settings
    )

    scraping_host.pipeline_runner.scraping_resource.add_limit(2)

    # https://dlthub.com/docs/general-usage/incremental-loading#scd2-strategy
    scraping_host.pipeline_runner.scraping_resource.apply_hints(merge_key="id_hash")
    scraping_host.run(
        columns=FundingProgramSchema,
        write_disposition={
            "disposition": "merge",
            "strategy": "scd2",
            "validity_column_names": ["on_website_from", "on_website_to"],
            "row_version_column_name": "checksum",
        },
    )

    columns = list(FundingProgramSchema.__annotations__.keys())

    query = f"""
    WITH aggregated_data AS (
        SELECT
            {dataset_name}.id_hash as agg_id,
            ARRAY_AGG({dataset_name}.on_website_from) AS update_dates
        FROM 
            {dataset_name}.{dataset_name}
        WHERE 
            {dataset_name}.on_website_to IS NULL
        GROUP BY 
            {dataset_name}.id_hash
    )
    SELECT
        {", ".join(columns)},
        aggregated_data.update_dates,
        {dataset_name}.on_website_from AS last_updated
    FROM 
        {dataset_name}.{dataset_name}
    JOIN 
        aggregated_data
        ON {dataset_name}.id_hash = aggregated_data.agg_id
    """
    df = pl.read_database(
        query=query,
        connection=engine.connect(),  # PostgreSQL connection string
        execute_options={"parameters": []},  # You can add parameters if necessary
    )
    df.write_parquet(local_data_file_name)

    # Step 1: Write the LICENSE content to a file
    with open(local_license_file_name, "w") as f:
        f.write(license_content)

    with zipfile.ZipFile(local_zip_file_name, "w") as zipf:
        zipf.write(
            local_data_file_name, os.path.basename(local_data_file_name)
        )  # Add the data file
        zipf.write(
            local_license_file_name, os.path.basename(local_license_file_name)
        )  # Add the LICENSE file

    session = boto3.session.Session()

    client = session.client(
        "s3",
        endpoint_url=os.getenv("DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL"),
        aws_access_key_id=os.getenv(
            "DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID"
        ),
        aws_secret_access_key=os.getenv(
            "DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY"
        ),
    )

    print(
        f"Uploading {local_zip_file_name} to {remote_file_name} in bucket {bucket_name}"
    )

    client.upload_file(local_zip_file_name, bucket_name, remote_file_name)

    # client.upload_file(
    #     local_zip_file_name, bucket_name, remote_file_name, ExtraArgs={"ACL": "public-read"}
    # )
