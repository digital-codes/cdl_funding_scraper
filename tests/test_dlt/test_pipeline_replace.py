from funding_crawler.spider import FundingSpider
import dlt
import os
import pytest
from funding_crawler.dlt_utils.helpers import create_pipeline_runner, cfg_provider
from funding_crawler.models import FundingProgramSchema
import warnings


@pytest.mark.filterwarnings(
    "error::pytest.PytestUnhandledThreadExceptionWarning"
)  # validation is not catched otherwise
def test_pipeline():
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

    scraping_host.run(
        columns=FundingProgramSchema,
        write_disposition="replace",
    )

    with pipeline.sql_client() as c:
        with c.execute_query("SELECT * FROM testdataset") as cur:
            rows = list(cur.fetchall())
            print(len(rows))
            assert len(rows) > 0

    os.remove("testpipeline.duckdb")

    del pipeline, scraping_host
