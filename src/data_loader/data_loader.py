import logging

from pyspark.sql import DataFrame


logger = logging.getLogger(__name__)


class DataLoader:
    """Responsible for loading data from a specified source."""

    def __init__(self, url: str):
        """Initialize the DataLoader with a data URL."""
        self.data_url = url

    def load_data(self) -> DataFrame:
        """Load data from the specified URL into a DataFrame."""
        try:
            df = spark.read.format("delta").load(self.data_url)
            logger.info("Data loaded successfully.")
            return df
        except Exception as e:
            logger.error(f"Error loading data: {e}", exc_info=True)
            raise
