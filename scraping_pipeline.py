import dlt
from funding_crawler.scrapy_utils import FundingSpider
from funding_crawler.dlt_utils import run_pipeline
from funding_crawler.dlt_utils.helpers import cfg_provider
from scrapy_settings import scrapy_settings
from funding_crawler.models import FundingProgramSchema


if __name__ == "__main__":
    dlt.config.register_provider(cfg_provider)

    pipeline = dlt.pipeline(
        pipeline_name="funding_crawler",
        destination="duckdb",
        dataset_name="main",
    )

    run_pipeline(
        pipeline,
        FundingSpider,
        # you can pass scrapy settings overrides here
        scrapy_settings=scrapy_settings,
        write_disposition="replace",
        columns=FundingProgramSchema,
    )
