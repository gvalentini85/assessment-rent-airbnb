import logging

from pyspark.sql import DataFrame, functions

logger = logging.getLogger(__name__)


class DataProcessor:
    """Handles data processing including cleaning and transformations."""

    def __init__(self):
        self.airbnb = None
        self.rentals = None
        self.output = None

    def clean_airbnb(self, df: DataFrame) -> DataFrame:
        """Generate the silver layer for AirBnB data."""
        try:
            self.airbnb = df
            return None
        except Exception as e:
            logger.error(f"Error processing AirBnB data: {e}", exc_info=True)
            raise

    def clean_rentals(self, df: DataFrame) -> DataFrame:
        """Generate the silver layer for rentals data."""
        try:
            self.rentals = df
            return None
        except Exception as e:
            logger.error(f"Error processing rentals data: {e}", exc_info=True)
            raise

    def aggregate_data(self) -> DataFrame:
        """
        Aggregate the silver DataFrame to generate the gold layer, focusing on
        key metrics.
        """

        try:
            self.output = self.airbnb
            logger.info("Gold data processed successfully.")
            return self.output.copy()
        except Exception as e:
            logger.error(f"Error aggregating data: {e}", exc_info=True)
            raise
