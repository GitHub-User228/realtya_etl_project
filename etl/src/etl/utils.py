import os
import yaml
import types
import typing
import datetime
import functools
import numpy as np
import pandas as pd
from typing import *
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine

from etl import logger


class EnsureAnnotation:

    @staticmethod
    def message1(
        func_name: str, arg_name: str, arg_type: type, valid_type: type
    ) -> str:
        """Generates a custom error message

        Args:
            func_name (str):
                Function's name
            arg_name (str):
                Argument's name
            arg_type (type):
                Type of the passed value for the argument
            valid_type (type):
                Excepted type of the passed value for the argument

        Returns:
            str:
                Error message
        """
        msg = (
            f"An exception occured while validating arguments of "
            + f"function '{func_name}'. Arg '{arg_name}' type must "
            + f"be {valid_type}, but got type {arg_type}"
        )
        return msg

    @staticmethod
    def message2(
        func_name: str, arg_name: str, arg_type: type, valid_types: list[type]
    ) -> str:
        """Generates a custom error message

        Args:
            func_name (str):
                Function's name
            arg_name (str):
                Argument's name
            arg_type (type):
                Type of the passed value for the argument
            valid_types (list[type]):
                List of excepted type of the passed value for the argument

        Returns:
            str:
                Error message
        """
        msg = (
            f"An exception occured while validating arguments of "
            + f"function '{func_name}'. Arg '{arg_name}' must have "
            + f"subvalues of type(s) {valid_types}, but got type {arg_type}"
        )
        return msg

    @staticmethod
    def message3(
        func_name: str, arg_name: str, arg_value: Any, valid_values: list[Any]
    ) -> str:
        """Generates a custom error message

        Args:
            func_name (str):
                Function's name
            arg_name (str):
                Argument's name
            arg_value (Any):
                Passed value for the argument
            valid_values (list[Any]):
                Excepted values for the argument

        Returns:
            str:
                Error message
        """
        msg = (
            f"An exception occured while validating arguments of "
            + f"function '{func_name}'. Arg '{arg_name}' must have a "
            + f"value from {valid_values}, but got value {arg_value} "
            + f"with type {type(arg_value)}"
        )
        return msg

    def check(self, func: Callable, arg_name: str, arg_value: Any, raise_error: bool):
        """
        Checks if the passed value of the argument is valid according
        to the annotation

        Args:
            func (Callable):
                Function for which an argument is to be checked
            arg_name (str):
                Argument's name
            arg_value (Any):
                Passed value for the argument
            raise_error (bool):
                Whether to raise ValueError if any argument has a non-valid
                type. If False - a corresponing warning message will be
                just logged

        Returns:
            bool:
                If True - a passed value is valid, else - not valid
        """

        # check arg if it's type must be a generic type
        if isinstance(func.__annotations__[arg_name], types.GenericAlias):

            # checking the origin type of the corresponding value
            if not isinstance(arg_value, func.__annotations__[arg_name].__origin__):
                msg = self.message1(
                    func.__name__,
                    arg_name,
                    type(arg_value),
                    func.__annotations__[arg_name],
                )
                if raise_error:
                    logger.error(msg)
                    raise ValueError(msg)
                else:
                    logger.warning(msg)
                    return False
            else:
                # checking subvalues if multiple types are possible
                if isinstance(
                    func.__annotations__[arg_name].__args__[0], types.UnionType
                ):
                    true_types = func.__annotations__[arg_name].__args__[0].__args__
                    for subvalue in arg_value:
                        if type(subvalue) not in true_types:
                            msg = self.message2(
                                func.__name__, arg_name, type(subvalue), true_types
                            )
                            if raise_error:
                                logger.error(msg)
                                raise ValueError(msg)
                            else:
                                logger.warning(msg)
                                return False

                # checking subvalues if a single type is possible
                else:
                    for subvalue in arg_value:
                        if not isinstance(
                            subvalue, func.__annotations__[arg_name].__args__[0]
                        ):
                            msg = self.message2(
                                func.__name__,
                                arg_name,
                                type(subvalue),
                                func.__annotations__[arg_name].__args__[0],
                            )
                            if raise_error:
                                logger.error(msg)
                                raise ValueError(msg)
                            else:
                                logger.warning(msg)
                                return False

        # check arg if it's type is typing._LiteralGenericAlias
        elif isinstance(func.__annotations__[arg_name], typing._LiteralGenericAlias):

            # check if value is from specified set of values
            if arg_value not in func.__annotations__[arg_name].__args__:
                msg = self.message3(
                    func.__name__,
                    arg_name,
                    arg_value,
                    func.__annotations__[arg_name].__args__,
                )
                if raise_error:
                    logger.error(msg)
                    raise ValueError(msg)
                else:
                    logger.warning(msg)
                    return False

        # check arg if it's type must be a single type
        else:
            if not isinstance(arg_value, func.__annotations__[arg_name]):
                msg = self.message1(
                    func.__name__,
                    arg_name,
                    type(arg_value),
                    func.__annotations__[arg_name],
                )
                if raise_error:
                    logger.error(msg)
                    raise ValueError(msg)
                else:
                    logger.warning(msg)
                    return False

        return True


def ensure_annotations(
    raise_error: bool = True, default_replacement: Any = None
) -> Callable:
    """
    Decorator function to ensure that all arguments correspond to the
    specified annotation. Usage limitations:
        - Checks annotations with a non-generic type (like str, list, int)
        - Checks annotations with a generic type (like list[str]), where
            each subtype is non-generic
    In all other cases this decorator probably won't work

    Args:
        raise_error (bool, default True):
            Whether to raise ValueError if any argument has a non-valid
            type. If False - a corresponing warning message will be
            logged and the function will return None
        default_replacement (Any, default None):
            The default value to be returned if any argument has a
            non-valid type

    Returns:
        function's call result | Any:
            Result of the function's call if
            all arguments have valid type. Returns default_replacement
            if any argument has a non-valid type and raise_error=False
    """

    if not isinstance(raise_error, bool):
        message = "raise_error must be a boolean value"
        logger.error(message)
        raise ValueError(message)

    def decorator(func: Callable) -> Callable:
        # functions to generate an error message

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:

            # Initialisation of the checker
            checker = EnsureAnnotation()

            # Checking if 'self' argument if exists
            varnames = list(func.__code__.co_varnames)
            i_start = 0
            if "self" in varnames:
                i_start = 1

            # checking each passed argument in the loop
            for it in range(i_start, len(args)):
                arg, value = varnames[it], args[it]

                # checking if argument has an annotation specified
                if arg in func.__annotations__:
                    is_valid = checker.check(
                        func=func,
                        arg_name=arg,
                        arg_value=value,
                        raise_error=raise_error,
                    )
                    if not is_valid:
                        return default_replacement

            # checking each passed key-word argument in the loop
            for kwarg, value in kwargs.items():

                # checking if argument has an annotation specified
                if kwarg in func.__annotations__:
                    is_valid = checker.check(
                        func=func,
                        arg_name=kwarg,
                        arg_value=value,
                        raise_error=raise_error,
                    )
                    if not is_valid:
                        return default_replacement

            # returning the result of the function if all arguments are valid
            return func(*args, **kwargs)

        return wrapper

    return decorator


@ensure_annotations()
def read_yaml(path: Path, verbose: bool = True) -> dict:
    """
    Reads a yaml file, and returns a dict.

    Args:
        path_to_yaml (Path):
            Path to the yaml file
        verbose (bool, default True):
            Whether to show logger's messages

    Returns:
        dict:
            The yaml content as a dict.
    """
    try:
        with open(path) as yaml_file:
            content = yaml.safe_load(yaml_file)
            if verbose:
                logger.info(f"yaml file: {path} loaded successfully")
            return content
    except Exception as e:
        logger.error(f"An exception has occurred while reading yaml. Error: {e}")
        raise e


@ensure_annotations()
def save_txt(data: list[str], path: Path, verbose: bool = True):
    """
    Saves list of strings to txt file

    Args:
        data (list[str]):
            Data to be saved
        path (Path):
            Path to the txt file
        verbose (bool, default True):
            Whether to show logger's messages
    """
    try:
        with open(path, "w") as txt_file:
            txt_file.write("\n".join(data))
            if verbose:
                logger.info(f"txt file: data successfully saved at {path}")
    except Exception as e:
        logger.error(f"An exception has occurred while saving txt data. Error: {e}")
        raise e


@ensure_annotations()
def read_txt(path: Path, verbose: bool = True) -> list[str]:
    """
    Reads a txt file, and returns data as a list of strings.

    Args:
        path (Path):
            Path to the txt file
        verbose (bool, default True):
            Whether to show logger's messages

    Returns:
        list[str]:
            The txt file content as a list of strings.
    """
    try:
        with open(path, "r") as txt_file:
            content = txt_file.read().split("\n")
            if verbose:
                logger.info(f"txt file: {path} loaded successfully")
            return content
    except Exception as e:
        logger.error(f"An exception has occurred while reading txt. Error: {e}")
        raise e


@ensure_annotations()
def create_connection_engine(is_source_db: bool = False) -> Engine:
    """
    Creates a connection engine to a database.

    Args:
        is_source_db (bool, default False):
            Whether to draw table from the source database

    Returns:
        Engine:
            Connection engine to the database
    """
    # Establishing the connection to a database
    if is_source_db:
        db_conn = (
            f'postgresql+psycopg2://{os.environ["POSTGRES_USER"]}'
            + f':{os.environ["POSTGRES_PASSWORD"]}'
            + f'@{os.environ["POSTGRES_HOST_SOURCE"]}'
            + f':{os.environ["POSTGRES_PORT_SOURCE"]}'
            + f'/{os.environ["POSTGRES_DB_SOURCE"]}'
        )
    else:
        db_conn = (
            f'postgresql+psycopg2://{os.environ["POSTGRES_USER"]}'
            + f':{os.environ["POSTGRES_PASSWORD"]}'
            + f'@{os.environ["POSTGRES_HOST_DESTINATION"]}'
            + f':{os.environ["POSTGRES_PORT_DESTINATION"]}'
            + f'/{os.environ["POSTGRES_DB_DESTINATION"]}'
        )
    try:
        engine = create_engine(db_conn)
        return engine
    except Exception as e:
        logger.info(
            f"An exception occured while establishing a connection to the "
            + f'{"source" if is_source_db else "destination"} database. Error: {e}'
        )
        raise e


@ensure_annotations()
def read_table_from_database(
    table_name: str,
    is_source_db: bool = False,
    date: Literal["last", "current", "all"] = "all",
) -> pd.DataFrame | None:
    """
    Reads table from either source or destination database

    Args:
        table_name (str):
            Name of the table to be read
        is_source_db (bool, default False):
            Whether to draw table from the source database
        date ({'last', 'current', 'all'}, default 'all'):
            Specific date at which data will be read
                - last: Data at the last date
                - current: Data at the current date
                - all: All data

    Returns:
        pd.DataFrame:
            Data from the database
    """
    # Creating a connection engine
    engine = create_connection_engine(is_source_db=is_source_db)

    # Specifying the select query depending on the 'date' argument
    query = f"SELECT * FROM {table_name}"
    if date == "current":
        query += f" WHERE date_parsed='{datetime.datetime.now().strftime('%Y-%m-%d')}'"
    elif date == "last":
        query += f" WHERE date_parsed=(SELECT MAX(date_parsed) FROM realty);"

    # Reading data from a database
    try:
        df = pd.read_sql_query(sql=query, con=engine)
        logger.info(
            f"Table {table_name} has been read from the "
            + f'{"source" if is_source_db else "destination"} database'
        )
    except Exception as e:
        logger.info(
            f"An exception occured while reading table {table_name} from the "
            + f'{"source" if is_source_db else "destination"} database. Error: {e}'
        )
        raise e

    # Returning data as a Pandas DataFrame
    return df


@ensure_annotations()
def save_data_to_database(
    df: pd.DataFrame,
    table_name: str,
    if_exists: Literal["fail", "replace", "append"] = "fail",
    index: bool = False,
    is_source_db: bool = False,
):
    """
    Reads table from either source or destination database

    Args:
        df (pd.DataFrame):
            Data to be saved
        table_name (str):
            Name of the table to be saved
        if_exists ({"fail", "replace", "append"}, default 'fail'):
            How to behave if the table already exists
            - fail: Raise a ValueError
            - replace: Drop the table before inserting new values
            - append: Insert new values to the existing table
        index (bool, default False):
            Whether to save the index column
        is_source_db (bool, default False):
            Whether to save data to the source database
    """
    # Creating a connection engine
    engine = create_connection_engine(is_source_db)

    # Saving data to a database
    try:
        df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=index)
        logger.info(
            f"Table {table_name} has been "
            + f'{"appended to the existing table" if if_exists=="append" else "saved"}'
            + f' to the {"source" if is_source_db else "destination"} database'
        )
    except Exception as e:
        logger.info(
            f"An exception occured while saving data to the table {table_name} "
            + f'of the {"source" if is_source_db else "destination"} database. '
            + f"Error: {e}"
        )
        raise e


@ensure_annotations()
def execute_sql_query(query: str, is_source_db: bool = False):
    """
    Executes a SQL query and returns the result

    Args:
        query (str):
            SQL query to be executed
        is_source_db (bool, default False):
            Whether to save data to the source database
    """
    # Creating a connection engine
    engine = create_connection_engine(is_source_db)

    # Processing SQL query
    with engine.connect() as connection:
        try:
            result = connection.execute(text(query))
            connection.commit()
            if result.returns_rows:
                # If the query returns rows, fetch the result
                rows = result.fetchall()
                if rows:
                    logger.info("Query executed successfully and returned rows.")
                    return rows
                else:
                    logger.info(
                        "Query executed successfully but did not return " + "any rows."
                    )
            else:
                # If the query does not return rows, check if it was successful
                if result.rowcount > 0:
                    logger.info(
                        "Query executed successfully and made changes "
                        + "to the database."
                    )
                else:
                    logger.info(
                        "Query executed successfully but did not make "
                        + "any changes to the database."
                    )
                return result.rowcount
        except Exception as e:
            logger.error(
                f"An exception occured while executing the SQL query. Error: {e}"
            )
            raise e


@ensure_annotations()
def boolean(arg: str):
    """
    Function to validate a bool argument passed to a python script

    Args:
        arg (str):
            Argument to be validated

    Returns:
        bool:
            A corresponding bool value (in case of the appropriate
            argument's value)
    """
    # print(len(arg))
    if arg == "True":
        output = True
    elif arg == "False":
        output = False
    else:
        error_message = (
            f"An exception occured while validating the boolean argument. "
            + f'The value is expected to be "True" or "False" but got "{arg}"'
        )
        logger.error(error_message)
        raise ValueError(error_message)
    return output


@ensure_annotations()
def get_bins(x: int) -> int:
    """
    Calculates the appropriate number of bins for the histogram
    according to the number of the observations

    Args:
        x (int):
            Number of the observations

    Returns:
        int:
            Number of bins
    """
    if x > 0:
        n_bins = max(int(1 + 3.2 * np.log(x)), int(1.72 * x ** (1 / 3)))
    else:
        message = (
            "An invalid input value passed. Expected a positive "
            + "integer, but got {x}"
        )
        logger.error(message)
        raise ValueError(message)
    return n_bins
