import dlt
from funding_crawler.scrapy_utils import FundingSpider
from funding_crawler.dlt_utils.helpers import create_pipeline_runner, cfg_provider
from scrapy_settings import scrapy_settings
from funding_crawler.models import FundingProgramSchema
import polars as pl
import boto3
from sqlalchemy import create_engine
import os
import modal


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
    schedule=modal.Cron("0 2  *  * *"),
    timeout=3600,
)
def crawl():
    dataset_name = "foerderdatenbankdumpbackend"
    bucket_name = "foerderdatenbankdump"

    local_file = "db.parquet"
    remote_file = "data/db.parquet"

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

    # scraping_host.pipeline_runner.scraping_resource.add_limit(2)

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

    df.write_parquet(local_file)

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

    print(f"Uploading {local_file} to {remote_file} in bucket {bucket_name}")

    client.upload_file(
        local_file, bucket_name, remote_file, ExtraArgs={"ACL": "public-read"}
    )

    os.remove(local_file)
