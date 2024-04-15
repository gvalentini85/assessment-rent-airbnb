import logging
from typing import List

from pyspark.shell import spark
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType
from shapely.geometry import Point, shape

from pyspark.sql.functions import (  # isort:skip
    array,
    col,
    lit,
    mean,
    regexp_replace,
    udf,
    when,
)

logger = logging.getLogger(__name__)


class DataProcessor:
    """Handles data processing including cleaning and transformations."""

    def __init__(self, airbnb_occupancy_rate: float):
        self.airbnb_occupancy_rate = airbnb_occupancy_rate
        self.airbnb = None
        self.rentals = None
        self.post_codes = None
        self.amsterdam_codes = []
        self.prices = None
        self.avg_prices = None

    def clean_airbnb(
        self, df: DataFrame, amsterdam_zipcodes: List[str], post_codes: dict
    ) -> DataFrame:
        """Generate the silver layer for AirBnB data."""

        # def zipcode_from_coordinates(coordinate: float):
        #     latitude = coordinate[0]
        #     longitude = coordinate[1]
        #
        #     point = Point(longitude, latitude)
        #
        #     # Check each polygon to see if it contains the point
        #     for feature in post_codes["features"]:
        #         polygon = shape(feature["geometry"])
        #         if polygon.contains(point):
        #             return float(feature["properties"]["pc4_code"])
        #
        #     return None
        #
        # zipcode_from_coordinates_udf = udf(
        #     zipcode_from_coordinates, DoubleType()
        # )

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
                .withColumn(
                    "capacity",
                    when(col("capacity") > 5, 6).otherwise(col("capacity")),
                )
                .filter(col("type") == "Entire home/apt")
                .drop("bedrooms", "review_scores_value")
            )

            def zipcode_from_coordinates(
                latitude: float, longitude: float, geo_data: dict
            ):
                # Check each polygon to see if it contains the point
                point = Point(longitude, latitude)
                for feature in geo_data["features"]:
                    polygon = shape(feature["geometry"])
                    if polygon.contains(point):
                        return feature["properties"]["pc4_code"]
                return None

            # Fill missing zipcodes
            airbnb_missing_zipcode = self.airbnb.filter(
                col("zipcode").isNull()
            ).toPandas()
            airbnb_missing_zipcode["zipcode"] = airbnb_missing_zipcode.apply(
                lambda x: zipcode_from_coordinates(
                    x.latitude, x.longitude, post_codes
                ),
                axis=1,
            )

            # Complete filtering
            self.airbnb = (
                self.airbnb.filter(col("zipcode").isNotNull())
                .union(spark.createDataFrame(airbnb_missing_zipcode))
                .filter(col("zipcode").isin(amsterdam_zipcodes))
            )
            return self.airbnb
        except Exception as e:
            logger.error(f"Error processing AirBnB data: {e}", exc_info=True)
            raise

    def clean_rentals(
        self, df: DataFrame, amsterdam_zipcodes: List[str]
    ) -> DataFrame:
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
                .filter(col("type").isin(["Apartment"]))
                .filter(col("zipcode").isin(amsterdam_zipcodes))
            )

            return self.rentals
        except Exception as e:
            logger.error(f"Error processing rentals data: {e}", exc_info=True)
            raise

    def clean_post_codes(self, data: dict) -> [dict, List[str]]:
        """Generate the silver layer for post codes data."""
        try:
            new_features = []
            amsterdam_codes = []
            for i in range(len(data["features"])):
                city = data["features"][i]["properties"]["gem_name"]
                zipcode = data["features"][i]["properties"]["pc4_code"]
                if city == "Amsterdam":
                    new_features.append(data["features"][i])
                    amsterdam_codes.append(zipcode)

            data["features"] = new_features
            self.post_codes = data
            self.amsterdam_codes = amsterdam_codes

            return [data, amsterdam_codes]
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

            # Work on airbnb data: Compute normalized measures
            df_airbnb = self.airbnb

            zipcodes_to_keep = (
                df_airbnb.groupby("zipcode")
                .count()
                .filter(col("count") > 5)
                .rdd.map(lambda x: x.zipcode)
                .collect()
            )

            df_airbnb = (
                df_airbnb.withColumn(
                    "monthly_price",
                    365.0
                    * self.airbnb_occupancy_rate
                    * df_airbnb.price
                    / 12.0,
                )
                .withColumn(
                    "monthly_price_per_person",
                    self.airbnb_occupancy_rate
                    * 365.0
                    * df_airbnb.price
                    / (12.0 * df_airbnb.capacity),
                )
                .withColumnRenamed("price", "daily_price")
                .filter(col("zipcode").isin(zipcodes_to_keep))
                .filter(col("daily_price") < 5000)
                .select([cname for cname in cols_list])
            )

            # Work on rentals data: Compute normalized measures
            df_rentals = self.rentals

            zipcodes_to_keep = (
                df_rentals.groupby("zipcode")
                .count()
                .filter(col("count") > 5)
                .rdd.map(lambda x: x.zipcode)
                .collect()
            )

            df_rentals = (
                df_rentals.withColumn(
                    "daily_price", df_rentals.rent * 12.0 / 365.0
                )
                .withColumn(
                    "monthly_price_per_person",
                    df_rentals.rent / df_rentals.capacity,
                )
                .withColumnRenamed("rent", "monthly_price")
                .filter(col("zipcode").isin(zipcodes_to_keep))
                .select([cname for cname in cols_list])
            )

            # Compute final results
            self.prices = df_airbnb.union(df_rentals)

            logger.info("AirBnB and Kamernet data aggregated successfully.")
            return self.prices
        except Exception as e:
            logger.error(f"Error aggregating data: {e}", exc_info=True)
            raise

    def compute_average_by_zipcode(self) -> DataFrame:
        """
        Compute the average of daily and monthly prices for the aggregated
        data by zipcode.
        """

        try:
            # Compute final results
            self.avg_prices = self.prices.groupBy(["source", "zipcode"]).agg(
                mean("daily_price").alias("avg_daily_price"),
                mean("monthly_price").alias("avg_monthly_price"),
                mean("monthly_price_per_person").alias(
                    "avg_monthly_price_per_person"
                ),
            )
            return self.avg_prices
        except Exception as e:
            logger.error(f"Error aggregating data: {e}", exc_info=True)
            raise
