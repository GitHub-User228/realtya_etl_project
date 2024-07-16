import sys
import unittest
import pandas as pd
from unittest.mock import patch

from etl import logger
from etl.pipeline import run_etl_pipeline
from etl.utils import execute_sql_query, save_data_to_database


class TestETLPipeline(unittest.TestCase):

    def setUp(self):
        self.mocker = patch.object(sys.modules, "etl.run_etl_pipeline")
        self.mocker.start()

    def tearDown(self):
        self.mocker.stop()

    def test_no_raw_data_parsed(self):
        """Test when there is no raw data parsed"""
        # Mock the necessary functions
        self.mocker.patch("etl.run_etl_pipeline.logger.warning")
        self.mocker.patch("etl.run_etl_pipeline.logger.info")
        self.mocker.patch("etl.run_etl_pipeline.sys.exit")
        self.mocker.patch(
            "etl.run_etl_pipeline.parser.retrieve", return_value=pd.DataFrame()
        )
        # Run the ETL pipeline
        run_etl_pipeline(parse=True, transform=False)
        # Assert that the expected messages are logged and the system exits
        logger.warning.assert_called_once_with("There is no raw data parsed")
        logger.info.assert_called_once_with("=== ENDING ETL PIPELINE ===")
        sys.exit.assert_called_once()

    def test_no_raw_data_in_source_db(self):
        """
        Test when there is no raw data stored in the source database
        with the current date
        """
        # Mock the necessary functions
        self.mocker.patch("etl.run_etl_pipeline.logger.warning")
        self.mocker.patch("etl.run_etl_pipeline.logger.info")
        self.mocker.patch("etl.run_etl_pipeline.sys.exit")
        self.mocker.patch(
            "etl.run_etl_pipeline.read_table_from_database", return_value=pd.DataFrame()
        )
        # Run the ETL pipeline
        run_etl_pipeline(parse=False, transform=True)
        # Assert that the expected messages are logged and the system exits
        logger.warning.assert_called_once_with(
            "There is no raw data stored in the source database with the current date."
        )
        logger.info.assert_called_once_with("=== ENDING ETL PIPELINE ===")
        sys.exit.assert_called_once()

    def test_no_data_after_transformation(self):
        """Test when there is no data left after transformation"""
        # Mock the necessary functions
        self.mocker.patch("etl.run_etl_pipeline.logger.warning")
        self.mocker.patch("etl.run_etl_pipeline.logger.info")
        self.mocker.patch("etl.run_etl_pipeline.sys.exit")
        self.mocker.patch(
            "etl.run_etl_pipeline.parser.retrieve", return_value=pd.DataFrame()
        )
        self.mocker.patch(
            "etl.run_etl_pipeline.transformer.transform", return_value=pd.DataFrame()
        )
        # Run the ETL pipeline
        run_etl_pipeline(parse=True, transform=True)
        # Assert that the expected messages are logged and the system exits
        logger.warning.assert_called_once_with(
            "There is no data left after transformation"
        )
        logger.info.assert_called_once_with("=== ENDING ETL PIPELINE ===")
        sys.exit.assert_called_once()

    def test_no_parsing_no_transforming(self):
        """Test when neither parsing nor transforming is requested"""
        # Mock the necessary functions
        self.mocker.patch("etl.run_etl_pipeline.logger.warning")
        self.mocker.patch("etl.run_etl_pipeline.logger.info")

        # Run the ETL pipeline
        run_etl_pipeline(parse=False, transform=False)

        # Assert that the expected message is logged
        logger.warning.assert_called_once_with(
            "Neither parsing nor transforming is requested"
        )
        logger.info.assert_called_once_with("=== ENDING ETL PIPELINE ===")

    def test_default_arguments(self):
        """
        Test when the ETL pipeline is run with default arguments
        (parse=True, transform=True, overwrite_source=False,
        overwrite_destination=False)
        """
        # Mock the necessary functions
        self.mocker.patch("etl.run_etl_pipeline.logger.info")
        self.mocker.patch(
            "etl.run_etl_pipeline.parser.retrieve", return_value=pd.DataFrame()
        )
        self.mocker.patch(
            "etl.run_etl_pipeline.transformer.transform", return_value=pd.DataFrame()
        )
        self.mocker.patch("etl.run_etl_pipeline.execute_sql_query")
        self.mocker.patch("etl.run_etl_pipeline.save_data_to_database")

        # Run the ETL pipeline with default arguments
        run_etl_pipeline()

        # Assert that the expected messages are logged
        logger.info.assert_called_once_with("=== STARTING ETL PIPELINE ===")
        logger.info.assert_called_once_with("=== ENDING ETL PIPELINE ===")

        # Assert that the necessary functions are called with the expected arguments
        parser.retrieve.assert_called_once_with(return_data=True, save_data=False)
        transformer.transform.assert_called_once_with(
            df=self.mocker.ANY, return_data=True, save_data=False
        )
        execute_sql_query.assert_any_call(query=self.mocker.ANY, is_source_db=True)
        execute_sql_query.assert_any_call(query=self.mocker.ANY, is_source_db=False)
        save_data_to_database.assert_any_call(
            df=self.mocker.ANY,
            table_name=self.mocker.ANY,
            is_source_db=True,
            index=False,
            if_exists="append",
        )
        save_data_to_database.assert_any_call(
            df=self.mocker.ANY,
            table_name=self.mocker.ANY,
            is_source_db=False,
            index=False,
            if_exists="append",
        )


if __name__ == "__main__":
    unittest.main()
