#!/usr/local/bin/python3

import sys
from datetime import datetime

from etl import logger
from etl.parser import RealtyYaParser
from etl.transformer import RealtyYaTransformer
from etl.utils import (
    read_table_from_database,
    save_data_to_database,
    execute_sql_query,
    ensure_annotations,
)


@ensure_annotations()
def run_etl_pipeline(
    parse: bool = True,
    transform: bool = True,
    overwrite_source: bool = False,
    overwrite_destination: bool = False,
) -> None:
    """
    Runs the ETL pipeline

    Args:
        parse (bool, optional, default True):
            Whether to parse new data.
        transform (bool, optional, default True):
            Whether to transform data. If parse is True then newly
            parsed data is transformed, else - the parsed data at
            the current date if it exists.
        overwrite_source (bool, optional, default False):
            Whether to overwrite recently obtained source data,
            if it was obtained at the same day.
        overwrite_destination (bool, optional, default False):
            Whether to overwrite recently obtained destination data,
            if it was obtained at the same day.
    """

    # Getting the current date
    current_date = datetime.now().date().strftime("%Y-%m-%d")

    if (not parse) and (not transform):
        logger.warning(f"Neither parsing nor transforming is requested")
    else:
        logger.info("=== STARTING ETL PIPELINE ===")

        # Initializing parser and transformer
        parser = RealtyYaParser()
        transformer = RealtyYaTransformer()

        # Checking if it is required to parse new data
        if parse:

            # Extracting raw data
            df = parser.retrieve(return_data=True, save_data=False)

            # Checking if the raw data is empty
            if len(df) == 0:
                logger.warning("There is no raw data parsed")
                logger.info("=== ENDING ETL PIPELINE ===")
                sys.exit()

            # Deleting rows from the main table with the current date
            if overwrite_source:
                query = (
                    f"DELETE FROM {parser.config['main_table_name']} "
                    + f"WHERE date_parsed='{current_date}';"
                )
                _ = execute_sql_query(
                    query=query,
                    is_source_db=True,
                )
                logger.info(
                    f"Rows from {parser.config['main_table_name']} table "
                    + f"with date_parsed='{current_date}' have been deleted"
                )

            # Saving raw data to the source database
            save_data_to_database(
                df=df,
                table_name=parser.config["main_table_name"],
                is_source_db=True,
                index=False,
                if_exists="append",
            )

        # Checking if it is required to transform raw data
        if transform:

            # Checking if it is not required to parse new data
            # In that case the parsed data at the current date is used
            if not parse:
                df = read_table_from_database(
                    table_name=parser.config["main_table_name"],
                    is_source_db=True,
                    date="current",
                )
                # Checking if the data is empty
                if len(df) == 0:
                    logger.warning(
                        "There is no raw data stored in the source database with "
                        + "the current date."
                    )
                    logger.info("=== ENDING ETL PIPELINE ===")
                    sys.exit()

            # Transforming the raw data
            df = transformer.transform(df=df, return_data=True, save_data=False)

            # Check if the transformed data is not empty
            if len(df) == 0:
                logger.warning("There is no data left after transformation")
                logger.info("=== ENDING ETL PIPELINE ===")
                sys.exit()

            # Deleting rows from the main table with the current date
            if overwrite_destination:
                query = (
                    f"DELETE FROM {transformer.config['main_table_name']} "
                    + f"WHERE date_parsed='{current_date}';"
                )
                _ = execute_sql_query(
                    query=query,
                    is_source_db=False,
                )
                logger.info(
                    f"Rows from {transformer.config['main_table_name']} table "
                    + f"with date_parsed='{current_date}' have been deleted"
                )

            # Saving transformed data to the destination database
            save_data_to_database(
                df=df,
                table_name=transformer.config["main_table_name"],
                is_source_db=False,
                index=False,
                if_exists="append",
            )

        logger.info("=== ENDING ETL PIPELINE ===")
