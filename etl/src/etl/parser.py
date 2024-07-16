#!/usr/local/bin/python3

import re
import time
import random
import requests
import pandas as pd
from tqdm.auto import tqdm
from datetime import datetime
from bs4 import BeautifulSoup

from etl import logger, PROXIES_PATH, CONFIG_PATH, LOG_PATH
from etl.utils import (
    save_txt,
    read_txt,
    read_yaml,
    save_data_to_database,
    ensure_annotations,
)


def scrape_proxies():
    """Retrieves available free proxies and saves them in the txt file"""
    url = "https://free-proxy-list.net/"
    with requests.Session() as s:
        response = s.get(url, timeout=5)
    bs = BeautifulSoup(response.text, "html.parser")
    data = list(map(lambda q: q.text, bs.find_all("td")))
    proxies = []
    i = 0
    while i * 8 + 1 < len(data):
        if re.match(r"\d+\.\d+\.\d+\.\d+", data[i * 8]) == None:
            break
        proxies.append(f"{data[i*8]}:{i*8+1}")
        i += 1
    save_txt(data=proxies, path=PROXIES_PATH, verbose=True)


class RealtyYaParser:
    """
    Class with the implementation of the custom parser for the content
    from https://realty.ya.ru/sankt-peterburg/snyat/kvartira/
    """

    def __init__(self):
        """
        Initializes RealtyYaParser

        Parameters:
            config (dict):
                Dictionary with the config
            proxies (list[str]):
                List with proxies (if they are required)
        """
        self.config = read_yaml(path=CONFIG_PATH)["extraction"]
        if self.config["use_proxy"]:
            self.proxies = read_txt(path=PROXIES_PATH, verbose=True)

    @ensure_annotations(False)
    def get(self, url: str) -> requests.models.Response | None:
        """
        Requesting content for the specified url

        Args:
            url (str):
                Url from which the content must be parsed

        Returns:
            requests.models.Response | None:
                Reponse from the url. Returns None if there is no
                successive response.
        """
        # Looping until a successful response or the maximum number
        # of tries is reached
        for _ in range(self.config["number_of_tries"]):
            proxy = None
            # Getting a proxy if necessary
            if self.config["use_proxy"]:
                proxy = self.proxies[random.randint(0, len(self.proxies) - 1)]
                proxy = {"http": proxy, "https": proxy}
            # Trying to get a response
            try:
                with requests.Session() as s:
                    response = s.get(
                        url=url,
                        timeout=self.config["waiting_time"],
                        headers=self.config["headers"],
                        proxies=proxy,
                    )
                    response.raise_for_status()
                # Returning the response after a successful request
                return response
            except:
                continue

    @ensure_annotations(False)
    def parse(
        self, response: requests.models.Response
    ) -> list[list[str | None]] | list:
        """
        Parses necessary content using the input response

        Args:
            response (requests.models.Response):
                Response

        Returns:
            list[list[str | None]] | list:
                Parsed content
        """
        # Getting the content using BeautifulSoup
        bs = BeautifulSoup(response.text, self.config["bs_parser"])
        # Parsing information for each parsing field
        content = []
        for cfg in self.config["parsing_fields"]["sub_fields"].values():
            content_ = bs.find_all(name=cfg["tag"], class_=cfg["classes"])
            if len(content_) == 0:
                content_ = None
            else:
                content_ = list(map(lambda x: x.text, content_))
                if cfg["return_first_parsed"]:
                    content_ = content_[0]
            content.append(content_)
        return content

    @ensure_annotations()
    def retrieve(
        self, return_data: bool = False, save_data: bool = False
    ) -> pd.DataFrame | None:
        """
        Requests the content from the specific url (see class
        definition above) and parses necessary information

        Args:
            return_data (bool, optional): Whether to return parsed data.
                Defaults to False.
            save_data (bool, optional): Whether to save parsed data.
                Defaults to False.

        Returns:
            pd.DataFrame | None: Parsed content as Pandas DataFrame
                (if return_data=True)
        """

        logger.info(f"STARTING PARSING STAGE")

        # Inititialisation of the list to store parsed data
        content = [
            None
            for _ in range(
                self.config["max_number_of_offers_per_page"]
                * self.config["number_of_pages"]
            )
        ]

        # Inititialisation of the tqdm iterator with a postfix over the pages
        iterator = tqdm(
            iterable=range(self.config["number_of_pages"]),
            file=open(f"{LOG_PATH}/running_logs.log", "a"),
        )
        d = {"content_size": 0, "skipped": 0}
        iterator.set_postfix(d)

        # Parsing each page in the loop
        for page in iterator:

            # Retrieving urls to all available offers
            response = self.get(url=f"{self.config['url']}?page={page}")
            offers = BeautifulSoup(response.text, self.config["bs_parser"]).find_all(
                name=self.config["parsing_fields"]["main_field"]["tag"],
                class_=self.config["parsing_fields"]["main_field"]["classes"],
            )
            offers = list(map(lambda x: x.get_attribute_list("href")[0], offers))

            # Parsing each offer separately
            for offer in offers:

                # getting a response for the current offer
                response = self.get(url=f"{self.config['offers_url']}{offer}")

                # Parsing data
                content_ = self.parse(response=response)

                # Updating counters & checking if anything was parsed
                if content_ == None:
                    d["skipped"] += 1
                    with open(f"{LOG_PATH}/running_logs.log", "a") as f:
                        f.write("\n")
                    logger.info(
                        f"URL = {self.config['offers_url']}{offer} : "
                        + "unable to parse content"
                    )
                else:
                    content_.append(offer.split("/")[-2])
                    content[d["content_size"]] = content_
                    d["content_size"] += 1

                # Timeout between requests
                time.sleep(self.config["timeout_between_requests"])

            # Updating tqdm postfix
            iterator.set_postfix(d)

        # Logging the final status
        with open(f"{LOG_PATH}/running_logs.log", "a") as f:
            f.write("\n")
        logger.info(f"Number of parsed observations: {d['content_size']}")
        logger.info(f"Number of skipped observations: {d['skipped']}")
        logger.info(f"Number of viewed pages: {self.config['number_of_pages']}")

        # Creating DataFrame with parsed data
        content = pd.DataFrame(
            data=content[: d["content_size"]],
            columns=list(self.config["parsing_fields"]["sub_fields"].keys())
            + ["offer_id"],
        )
        content["offer_id"] = content["offer_id"].astype("int64")

        # Dropping duplicates (if any) based on offer_id column
        content = content.drop_duplicates(subset="offer_id")

        # Adding date_parsed column to the DataFrame
        content["date_parsed"] = datetime.now().date().strftime("%Y-%m-%d")

        # Saving DataFrame to the postgres database file if required
        if save_data:
            save_data_to_database(
                df=content,
                table_name=self.config["main_table_name"],
                is_source_db=True,
                index=False,
                if_exists="append",
            )
        logger.info(f"ENDING PARSING STAGE")

        # Returning DataFrame if required
        if return_data:
            return content
