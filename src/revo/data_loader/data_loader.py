import json
import logging
import pathlib
from typing import Union
from urllib.request import urlopen

from pyspark.shell import spark
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class DataLoader:
    """Responsible for loading data from a specified source."""

    def __init__(self, url: str):
        """Initialize the DataLoader with a data URL."""
        self.data_url = url

    def load_data(self) -> Union[DataFrame, dict]:
        """Load data from the specified URL into a DataFrame."""
        try:
            data_format = pathlib.Path(self.data_url).suffix
            if data_format == ".json":
                df = self._load_json()
            elif data_format == ".geojson":
                df = self._load_geojson()
            elif data_format == ".csv":
                df = self._load_csv()
            else:
                logger.error("Unsupported format", exc_info=True)
                raise
            logger.info("Data loaded successfully.")
            return df
        except Exception as e:
            logger.error(f"Error loading data: {e}", exc_info=True)
            raise

    def _load_csv(self) -> DataFrame:
        """Load data from a csv file into a DataFrame."""
        return spark.read.option("header", True).csv(self.data_url)

    def _load_json(self) -> DataFrame:
        """Load data from a csv file into a DataFrame."""
        return spark.read.json(self.data_url)

    def _load_geojson(self) -> dict:
        """Load data from a csv file and keep the original json format."""
        with urlopen(self.data_url) as response:
            data = json.load(response)
        return data
