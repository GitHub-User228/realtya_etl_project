import re
import sys
import time
import pandas as pd
from geopy.geocoders import ArcGIS

from etl import logger, CONFIG_PATH
from etl.utils import (
    read_yaml,
    read_table_from_database,
    save_data_to_database,
    ensure_annotations,
)


CONFIG = read_yaml(path=CONFIG_PATH)["transformation"]["features"]


@ensure_annotations(False, [None] * len(CONFIG["flat_type"]["features"]))
def transform_flat_type(raw_content: str) -> list[int | bool | None]:
    """
    Transforms a single record of the flat_type field into the
    specified features (see config)

    Args:
        raw_content (str):
            Record to be transformed

    Returns:
        list[int | bool | None]:
            List of obtained features
    """
    content = [None for _ in range(len(CONFIG["flat_type"]["features"]))]
    if "студия" in raw_content:
        content[1] = True
    else:
        content[1] = False
    output = re.findall(r"\d+\-комн", raw_content)
    if len(output) == 1:
        content[0] = int(re.match(r"\d+", output[0]).group())
    elif content[1] == True:
        content[0] = 1
    return content


@ensure_annotations(False, [None] * len(CONFIG["main_info"]["features"]))
def transform_main_info(raw_content: list[str]) -> list[float | int | None]:
    """
    Transforms a single record of the main_info field into the
    specified features (see config)

    Args:
        raw_content (list[str]):
            Record to be transformed

    Returns:
        list[float | int | None]:
            List of obtained features
    """
    content = dict([(k, None) for k in CONFIG["main_info"]["features"]])
    for text in raw_content:
        if "общая" in text:
            content["area"] = float(".".join(re.findall(r"\d+", text)))
        elif "этаж" in text:
            vals = list(map(int, re.findall(r"\d+", text)))
            content["floor"] = vals[0]
            if len(vals) == 2:
                content["total_floors"] = vals[1]
        elif "потолки" in text:
            content["height"] = float(".".join(re.findall(r"\d+", text)))
        elif "год" in text:
            content["construction_year"] = int(re.findall(r"\d+", text)[0])
    content = list(content.values())
    return content


@ensure_annotations(False, [None] * len(CONFIG["fee_info"]["features"]))
def transform_fee_info(raw_content: list[str]) -> list[float | int | bool | None]:
    """
    Transforms a single record of the fee_info field into the
    specified features (see config)

    Args:
        raw_content (list[str]):
            Record to be transformed

    Returns:
        list[int | bool | None]:
            List of obtained features
    """
    content = [None for _ in range(len(CONFIG["fee_info"]["features"]))]  #
    if raw_content[0] == "есть":
        content[0] = True
    else:
        content[1] = int("".join(re.findall(r"\d+", raw_content[0])))
        if content[1] == 0:
            content[0] = False  #
    content[2] = int("".join(re.findall(r"\d+", raw_content[1])))  #
    content[3] = raw_content[2]  #
    content[4] = int("".join(re.findall(r"\d+", raw_content[3])))
    return content


@ensure_annotations()
def transform_address_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms address_info field into the specified features (see
    config)

    Args:
        df (pd.DataFrame):
            Dataframe with the raw data for the address_info field

    Returns:
        pd.DataFrame:
            Dataframe with the obtained features for the address_info
            field
    """

    # Initialisation of the geolocator
    geolocator_arcgis = ArcGIS()

    @ensure_annotations(False, [None, None])
    def coords(address: str) -> list[float | None]:
        """
        Decodes an address in order to obtain it's location:
        latitude and longitude

        Args:
            address (str): Address

        Returns:
            list[float | None]:
                Obtained location (filled with None if it was
                not possible to locate the address)
        """
        # Getting coordinates for the address
        try:
            location = geolocator_arcgis.geocode(address)
        except:
            logger.warning(f"Unable to initially locate address '{address}'")
        time.sleep(0.1)
        # Checking if the address was not found
        if location == None:
            # Trying to apply existing adjustments to the address if it was not found
            for k, v in CONFIG["address_info"]["address_adjustment"].items():
                if k in address:
                    try:
                        location = geolocator_arcgis.geocode(f"{v}{address}")
                        break
                    except:
                        time.sleep(0.1)

            # Setting the location to Nones if it was not found after adjustments
            if location == None:
                logger.warning(
                    f"Unable to locate address '{address}' after adjustments"
                )
                location = [None, None]
            # Getting coordinates from the location
            else:
                location = location.point[:2]
        # Getting coordinates from the location
        else:
            location = location.point[:2]
        return location

    # Reading existing data with addresses
    df_a = read_table_from_database(
        table_name="addresses", is_source_db=True
    )  # .drop('id', axis=1)

    # Joining
    df = df.merge(df_a, how="left", on="address_info")

    # Checking if any new addresses exist in the data
    if len(df[df["latitude"].isnull() & df["address_info"].notnull()]) > 0:

        # Getting new addresses
        df_a2 = df[df["latitude"].isnull() & df["address_info"].notnull()][
            ["address_info"]
        ].drop_duplicates()

        # Getting coordinates for new addresses
        df_a2[["latitude", "longitude"]] = pd.DataFrame(
            df_a2["address_info"].map(coords).tolist(), index=df_a2.index
        )

        # Dropping data with not searchable coordinates
        df_a2 = df_a2.dropna(subset="latitude", axis=0)

        # New address_ids for new addresses
        df_a2["address_id"] = [len(df_a) + 1 + x for x in range(len(df_a2))]

        # Saving new address data to the addresses table
        save_data_to_database(
            df=df_a2,
            table_name="addresses",
            if_exists="append",
            index=False,
            is_source_db=True,
        )

        # Joining new addresses to the main dataframe
        df.loc[
            df["latitude"].isnull() & df["address_info"].notnull(),
            ["address_id", "latitude", "longitude"],
        ] = (
            df.loc[
                df["latitude"].isnull() & df["address_info"].notnull(), ["address_info"]
            ]
            .merge(df_a2, how="left", on="address_info")[
                ["address_id", "latitude", "longitude"]
            ]
            .values
        )

    df = df.drop("address_info", axis=1)

    return df


@ensure_annotations(False, [None] * len(CONFIG["extra_features"]["features"]))
def transform_extra_features(
    raw_content: list[str],
) -> list[bool | int | float | str | None]:
    """
    Transforms a single record of the extra_features field into
    the specified features (see config)

    Args:
        raw_content (list[str]):
            Record to be transformed

    Returns:
        list[bool | int | float | str | None]:
            List of obtained features
    """
    content = dict(
        [(k, v["default"]) for (k, v) in CONFIG["extra_features"]["features"].items()]
    )
    raw_content = set(raw_content)
    for field, subconfig in CONFIG["extra_features"]["features"].items():
        for item in raw_content:
            if item in subconfig["values"]:
                content[field] = subconfig["values"][item]
                raw_content.remove(item)
                break
    content = list(content.values())
    return content


class RealtyYaTransformer:
    """
    Class with the implementation of the custom data transformer for the
    content from https://realty.ya.ru/sankt-peterburg/snyat/kvartira/
    """

    def __init__(self):
        """
        Initializes RealtyYaTransformer

        Parameters:
            config (dict):
                dictionary with the config
        """
        self.config = read_yaml(path=CONFIG_PATH)["transformation"]

    @ensure_annotations()
    def transform(
        self, df: pd.DataFrame, return_data: bool = False, save_data: bool = False
    ) -> pd.DataFrame | None:
        """
        Transforms raw data

        Args:
            df (pd.DataFrame):
                Raw data
            return_data (bool, default False):
                Whether to return transformed data
            save_data (bool, default False):
                Whether to save transformed data

        Returns:
            pd.DataFrame | None:
                Transformed data as Pandas DataFrame (if return_data=True)
        """

        logger.info("STARTING TRANSFORMING STAGE")

        # Transforming each column separately
        for feature, config in self.config["features"].items():

            try:
                # Checking if the feature should be processed separately
                if config["process_per_observation"]:
                    if type(config["features"]) == dict:
                        features = list(config["features"].keys())
                    else:
                        features = config["features"]
                    df[features] = pd.DataFrame(
                        df[feature]
                        .apply(
                            lambda x: getattr(
                                sys.modules[__name__], config["transform_func"]
                            )(raw_content=x)
                        )
                        .tolist(),
                        index=df.index,
                    )
                    df.drop(feature, axis=1, inplace=True)

                # Feature should be processed as a whole
                else:
                    df = getattr(sys.modules[__name__], config["transform_func"])(df=df)
                logger.info(f"Feature {feature} has been transformed")

            except Exception as e:
                logger.info(
                    f"An exception occured while transforming feature "
                    + f"{feature}. Error: {e}"
                )
                raise e

        # Saving transformed data to the destination database
        if save_data:
            save_data_to_database(
                df=df,
                table_name=self.config["main_table_name"],
                is_source_db=False,
                index=False,
                if_exists="append",
            )

        logger.info("ENDING TRANSFORMING STAGE")

        # Returning transformed data if required
        if return_data:
            return df
