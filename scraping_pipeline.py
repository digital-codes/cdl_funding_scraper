import dlt
from funding_crawler.scrapy_utils import FundingSpider

# from funding_crawler.dlt_utils import run_pipeline
from funding_crawler.dlt_utils.helpers import create_pipeline_runner, cfg_provider
from scrapy_settings import scrapy_settings
from funding_crawler.models import FundingProgramSchema
import polars as pl
import boto3
import os

dataset_name = "foerderdatenbankdumpbackend"
bucket_name = "foerderdatenbankdump"

local_file = "db.parquet"
remote_file = "db.parquet"

if __name__ == "__main__":
    dlt.config.register_provider(cfg_provider)

    pipeline = dlt.pipeline(
        pipeline_name="funding_crawler",
        destination="filesystem",
        dataset_name=dataset_name,
    )

    scraping_host = create_pipeline_runner(
        pipeline, FundingSpider, batch_size=50, scrapy_settings=scrapy_settings
    )

    scraping_host.pipeline_runner.scraping_resource.add_limit(2)

    scraping_host.run(write_disposition="replace", columns=FundingProgramSchema)

    with pipeline.sql_client() as c:
        with c.execute_query(f"SELECT * FROM {dataset_name}.{dataset_name}") as cur:
            rows = list(cur.fetchall())
            print("\n\n", len(rows))
            assert len(rows) > 0

            df = pl.DataFrame(rows)
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

    try:
        os.remove(local_file)
        print(f"Local file '{local_file}' deleted successfully.")
    except Exception as e:
        print(f"Failed to delete local file: {e}")
        raise

    # run_pipeline(
    #     pipeline,
    #     FundingSpider,
    #     scrapy_settings=scrapy_settings,
    #     # https://dlthub.com/docs/tutorial/sql-database#load-with-merge
    #     primary_key="id_hash",
    #     write_disposition="merge",

    #     columns=FundingProgramSchema,
    # )
