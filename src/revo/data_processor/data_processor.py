import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, mean, regexp_replace, upper, when

logger = logging.getLogger(__name__)


class DataProcessor:
    """Handles data processing including cleaning and transformations."""

    def __init__(self):
        self.airbnb = None
        self.rentals = None
        self.post_codes = None
        self.output = None

    def clean_airbnb(self, df: DataFrame) -> DataFrame:
        """Generate the silver layer for AirBnB data."""
        try:
            cols_changes = {
                "zipcode": regexp_replace(col("zipcode"), "[^0-9]", ""),
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
                .withColumn("source", lit("airbnb"))
                .withColumnRenamed("room_type", "type")
                .withColumnRenamed("accommodates", "capacity")
                .drop("bedrooms", "review_scores_value")
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
                "postalCode": regexp_replace(col("postalCode"), "[^0-9]", ""),
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

    def clean_post_codes(self, df: DataFrame) -> DataFrame:
        """Generate the silver layer for post codes data."""
        try:
            self.post_codes = (
                df.drop("_corrupt_record").dropna().dropDuplicates()
            )

            return self.post_codes
        except Exception as e:
            logger.error(
                f"Error processing post codes data: {e}", exc_info=True
            )
            raise

    def aggregate_data(self) -> DataFrame:
        """
        Aggregate the silver DataFrames of AirBnB and Kamernet to generate
        the gold layer with price metrics.
        """

        try:
            cols_list = [
                "source",
                "zipcode",
                "daily_price",
                "monthly_price",
                "monthly_price_per_person",
            ]
            common_zipcodes = [
                row["zipcode"]
                for row in (
                    self.airbnb.select("zipcode")
                    .intersect(self.rentals.select("zipcode"))
                    .collect()
                )
            ]

            # Work on airbnb data:
            # Filter only entire home
            df_airbnb = self.airbnb.filter(col("type") == "Entire home/apt")

            # Filter common zipcodes
            df_airbnb = df_airbnb.filter(
                df_airbnb.zipcode.isin(common_zipcodes)
            )

            # Set capacity > 5 to 6 (i.e., group all capacity higher
            # than 5 together in a single category '6')
            df_airbnb = df_airbnb.withColumn(
                "capacity",
                when(col("capacity") > 5, 6).otherwise(col("capacity")),
            )

            # Compute normalized measures
            df_airbnb = (
                df_airbnb.withColumn(
                    "monthly_price", df_airbnb.price * 365.0 / 12.0
                )
                .withColumn(
                    "monthly_price_per_person",
                    df_airbnb.price * 365.0 / (12.0 * df_airbnb.capacity),
                )
                .withColumnRenamed("price", "daily_price")
                .select([cname for cname in cols_list])
            )

            # Work on rentals data:
            # Filter only entire apartment and studio
            df_rentals = self.rentals.filter(
                col("type").isin(["Apartment", "Studio"])
            )

            # Filter common zipcodes
            df_rentals = df_rentals.filter(
                df_rentals.zipcode.isin(common_zipcodes)
            )

            # Compute normalized measures
            df_rentals = (
                df_rentals.withColumn(
                    "daily_price", df_rentals.rent * 12.0 / 365.0
                )
                .withColumn(
                    "monthly_price_per_person",
                    df_rentals.rent / df_rentals.capacity,
                )
                .withColumnRenamed("rent", "monthly_price")
                .select([cname for cname in cols_list])
            )

            # Compute final results
            self.output = (
                df_airbnb.union(df_rentals)
                .groupBy(["source", "zipcode"])
                .agg(
                    mean("daily_price").alias("avg_daily_price"),
                    mean("monthly_price").alias("avg_monthly_price"),
                    mean("monthly_price_per_person").alias(
                        "avg_monthly_price_per_person"
                    ),
                )
            )
            logger.info("Gold data processed successfully.")
            return self.output
        except Exception as e:
            logger.error(f"Error aggregating data: {e}", exc_info=True)
            raise
