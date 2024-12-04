import dlt
from funding_crawler.spider import FundingSpider
from funding_crawler.dlt_utils.helpers import create_pipeline_runner, cfg_provider
from funding_crawler.helpers import gen_query
from scrapy_settings import scrapy_settings
from funding_crawler.models import FundingProgramSchema
import polars as pl
import boto3
import zipfile
from sqlalchemy import create_engine
import os
import modal

license_content = """
Inhalte von foerderdatenbank.de sind individuell lizensiert durch das Bundesministerium für Wirtschaft und Klimaschutz unter CC BY-ND 3.0 DE. 
Der hier zur Verfügung gestellte Datensatz gibt die Informationen zu jedem einzelnen Förderprogramms in einem maschinenlesbaren Format wider. 
Lizenzinformationen zu jedem einzenlnen Förderprogramm inkl. URL können der Datensatz-Spalte license_info sowie der Datei LICENSE-DATA entnommen werden.
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

    local_license_file_name = "LICENSE-DATA"
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

    df = pl.read_database(
        query=gen_query(dataset_name, columns),
        connection=engine.connect(),
        execute_options={"parameters": []},
    )

    df.write_parquet(local_data_file_name)

    with open(local_license_file_name, "w") as f:
        f.write(license_content)

    with zipfile.ZipFile(local_zip_file_name, "w") as zipf:
        zipf.write(local_data_file_name, os.path.basename(local_data_file_name))
        zipf.write(local_license_file_name, os.path.basename(local_license_file_name))

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
