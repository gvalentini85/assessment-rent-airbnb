import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, upper

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
            cols_changes = {
                "zipcode": regexp_replace(upper(col("zipcode")), "\\s+", ""),
                "latitude": col("latitude").cast("double"),
                "longitude": col("longitude").cast("double"),
                "accommodates": col("accommodates").cast("int"),
                "bedrooms": col("bedrooms").cast("int"),
                "price": col("price").cast("double"),
                "review_scores_value": col("review_scores_value").cast("int"),
            }

            self.airbnb = (
                df.dropDuplicates()
                .withColumns(cols_changes)
                .withColumnRenamed("room_type", "type")
                .withColumnRenamed("accommodates", "capacity")
            )
            return self.airbnb
        except Exception as e:
            logger.error(f"Error processing AirBnB data: {e}", exc_info=True)
            raise

    def clean_rentals(self, df: DataFrame) -> DataFrame:
        """Generate the silver layer for rentals data."""
        try:
            cols_list = [
                "postalCode",
                "latitude",
                "longitude",
                "propertyType",
                "matchCapacity",
                "rent",
                "source",
            ]

            cols_changes = {
                "latitude": col("latitude").cast("double"),
                "longitude": col("longitude").cast("double"),
                "matchCapacity": (
                    regexp_replace(
                        regexp_replace(
                            col("matchCapacity"), "> 5 persons", "6"
                        ),
                        "[^0-9]",
                        "",
                    ).cast("int")
                ),
                "rent": regexp_replace(col("rent"), "[^0-9]", "").cast(
                    "double"
                ),
            }

            self.rentals = (
                df.select([col_name for col_name in cols_list])
                .dropDuplicates()
                .withColumns(cols_changes)
                .withColumnRenamed("postalCode", "zipcode")
                .withColumnRenamed("propertyType", "type")
                .withColumnRenamed("matchCapacity", "capacity")
            )

            return self.rentals
        except Exception as e:
            logger.error(f"Error processing rentals data: {e}", exc_info=True)
            raise

    def aggregate_data(self) -> DataFrame:
        """
        Aggregate the silver DataFrame to generate the gold layer, focusing on
        key metrics.
        """

        try:
            # df.filter(df.room_type == "Entire home/apt")
            self.output = self.airbnb
            logger.info("Gold data processed successfully.")
            return self.output
        except Exception as e:
            logger.error(f"Error aggregating data: {e}", exc_info=True)
            raise
