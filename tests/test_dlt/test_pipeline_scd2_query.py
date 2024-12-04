import dlt
import os
import pytest
import polars as pl
from funding_crawler.dlt_utils.helpers import create_pipeline_runner, cfg_provider
from funding_crawler.helpers import gen_query
from funding_crawler.models import FundingProgramSchema
from funding_crawler.spider import FundingSpider
import warnings
import duckdb


@pytest.mark.filterwarnings(
    "error::pytest.PytestUnhandledThreadExceptionWarning"
)  # validation is not catched otherwise
def test_scd2_query():
    dlt.config.register_provider(cfg_provider)
    warnings.filterwarnings("ignore", category=DeprecationWarning)

    pipeline = dlt.pipeline(
        pipeline_name="testpipeline", destination="duckdb", dataset_name="testdataset"
    )

    scrapy_settings = {
        "HTTPERROR_ALLOW_ALL": False,
        "BOT_NAME": "cdl_awo_funding_crawler_test",
        "LOG_LEVEL": "INFO",
        "CONCURRENT_REQUESTS": 3,
        "LOGSTATS_INTERVAL": "5.0",
    }

    scraping_host = create_pipeline_runner(
        pipeline, FundingSpider, batch_size=10, scrapy_settings=scrapy_settings
    )
    scraping_host.pipeline_runner.scraping_resource.add_limit(10)

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

    dataset_name = "testdataset.testdataset"
    columns = list(FundingProgramSchema.__annotations__.keys())

    query = gen_query(dataset_name, columns)

    conn = duckdb.connect(database="testpipeline.duckdb", read_only=True)

    df = conn.sql(query).pl()

    assert len(df) > 0, "Query did not return any rows."

    first_row = df[0]

    assert first_row["id_hash"] is not None
    assert first_row["last_updated"] is not None

    assert isinstance(first_row["previous_update_dates"], pl.Series)

    os.remove("testpipeline.duckdb")
