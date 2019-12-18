"""Script to download options data from Think or Swim (ToS) API."""

# Standard libraries
import pickle
from datetime import datetime
import logging
import os
import time
from typing import Dict, List
import requests

# External dependencies
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

# Application-specific imports

# Constants
TOS_OPTION_CHAIN_API_URL = "https://api.tdameritrade.com/v1/marketdata/chains"
CBOE_SYMBOLS_URL = (
    "http://markets.cboe.com/us/options/symboldir/equity_index_options/?download=csv"
)


class OptionsDataDownloader:
    """OptionsDataDownloader downloads data from ToS API and stores it in a DB."""

    def __init__(self):
        self.session = requests.session()
        self.db_handle = None

    def connect_and_initialize_db(self):
        client = MongoClient()
        self.db_handle = client.options
        self.db_handle.options_data.create_index(
            [("dataDate", ASCENDING), ("symbol", ASCENDING)], unique=True
        )

    def get_and_pickle_data(self, symbols: List):
        today_str = datetime.now().strftime("%Y%m%d")
        try:
            os.mkdir(today_str)
        except FileExistsError:
            logging.info("%s directory already exists", today_str)
        for symbol in symbols:
            if [i for i in os.listdir(today_str) if i.startswith(symbol + "_")]:
                logging.warning("%s already present, skipping", symbol)
                continue
            data = self.get_option_chain_from_broker(symbol)
            if data["status"] == "FAILED":
                logging.warning("%s FAILED!", symbol)
                continue
            with open(
                today_str + "/" + symbol + "_" + today_str + "_data.pkl", "wb"
            ) as p_data:
                pickle.dump(data, p_data)

    def pickle_to_db(self, folder=None):
        self.connect_and_initialize_db()
        folder = datetime.now().strftime("%Y%m%d") if folder is None else folder
        number_of_docs_before = self.db_handle.options_data.count_documents({})
        pkls = [i for i in os.listdir(folder) if i.endswith(".pkl")]
        for pkl_file in pkls:
            with open(folder + "/" + pkl_file, "rb") as p_data:
                data = pickle.load(p_data)
            data["dataDate"] = pkl_file.split("_")[1]
            data = replace_dots_in_keys(data)
            try:
                insert_result = self.db_handle.options_data.insert_one(data)
            except DuplicateKeyError:
                logging.warning(
                    "Document for %s from %s already exists in DB",
                    data["symbol"],
                    data["dataDate"],
                )
                continue
            logging.info(
                "Inserted %s with id %s", data["symbol"], insert_result.inserted_id
            )
        number_of_docs_after = self.db_handle.options_data.count_documents({})
        logging.info(
            "Inserted %s new documents to DB",
            number_of_docs_after - number_of_docs_before,
        )

    def get_option_chain_from_broker(self, symbol: str) -> Dict:
        retries = 60
        while retries:
            try:
                response = self.session.get(
                    TOS_OPTION_CHAIN_API_URL
                    + "?apikey="
                    + os.environ.get("TOS_API_KEY")
                    + "&symbol="
                    + symbol
                    + "&strikeCount=512&includeQuotes=TRUE",
                    timeout=32,
                )
            except ConnectionError as error:
                logging.error("Failed getting option chain for %s: %s", symbol, error)
                retries = retries - 1
                time.sleep(2)
            data = response.json()
            try:
                if data["status"]:
                    return data
            except KeyError:
                if data["error"]:
                    logging.warning(data["error"])
                    retries = retries - 1
                    time.sleep(2)


def get_symbols() -> List[str]:
    rows = requests.get(CBOE_SYMBOLS_URL).text.splitlines()
    symbols = []
    for row in rows:
        try:
            symbol_candidate = row.split('","')[1]
        except IndexError:
            logging.info("Row is not parsable: %s", row)
            continue
        if "#" not in symbol_candidate:
            symbols.append(symbol_candidate)
    symbols = list(set(symbols))
    symbols.sort()
    logging.info("Got %s symbols", len(symbols))
    if len(symbols) < 9000:
        raise "Too few symbols. You should check what's going on"
    return symbols


def replace_dots_in_keys(dictionary: Dict) -> Dict:
    new_dict = {}
    for key, value in dictionary.items():
        if isinstance(value, Dict):
            value = replace_dots_in_keys(value)
        new_dict[key.replace(".", ",")] = value
    return new_dict


def main():
    logging.getLogger().setLevel(logging.INFO)
    options_data_downloader = OptionsDataDownloader()
    symbols = get_symbols()
    options_data_downloader.get_and_pickle_data(symbols)
    # Do it again in case some symbols weren't downloaded
    options_data_downloader.get_and_pickle_data(symbols)
    options_data_downloader.pickle_to_db()


if __name__ == "__main__":
    main()
