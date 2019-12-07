"""Script to download options data from Think or Swim (ToS) API."""

# Standard libraries
import pickle
from datetime import datetime
import sqlite3
import logging
import os
import time
from typing import Dict, List
import requests

# External dependencies

# Application-specific imports
from symbols import ALL_SYMBOLS, EXCLUDED_SYMBOLS
from tokens import API_KEY


# Constants
DB_URL = "./options-data.sqlite"
TOS_OPTION_CHAIN_API_URL = "https://api.tdameritrade.com/v1/marketdata/chains"


class OptionsDataDownloader:
    """OptionsDataDownloader downloads data from ToS API and stores it in a DB."""

    def __init__(self):
        self.session = requests.session()
        self.db_connection = sqlite3.connect(DB_URL)
        self.db_cursor = self.db_connection.cursor()

    def get_and_store_data(self, symbols: List, from_pickle: bool = False):
        for symbol in symbols:
            today_str = datetime.now().strftime("%Y%m%d")
            if from_pickle:
                with open(symbol + "_" + today_str + "_data.pkl", "rb") as p_data:
                    data = pickle.load(p_data)
            else:
                if [i for i in os.listdir(".") if i.startswith(symbol + "_")]:
                    logging.warning("%s already present, skipping", symbol)
                    continue
                data = self.get_option_chain_data(symbol)
                if data["status"] == "FAILED":
                    logging.warning("%s FAILED!", symbol)
                    continue
                with open(symbol + "_" + today_str + "_data.pkl", "wb") as p_data:
                    pickle.dump(data, p_data)

            for option_type in ("putExpDateMap", "callExpDateMap"):
                for date_str in data[option_type]:
                    date = datetime.strptime(date_str.split(":")[0], "%Y-%m-%d")
                    for strike_str in data[option_type][date_str]:
                        for entry in data[option_type][date_str][strike_str]:
                            type_char = "C" if option_type == "callExpDateMap" else "P"
                            option_symbol = (
                                symbol
                                + date.strftime("%y%m%d")
                                + type_char
                                + "{:09.3f}".format(float(strike_str)).replace(".", "")
                            )  # e.g.: NVDA190301C00085000
                            query_values = (
                                symbol,
                                data["underlying"]["last"],
                                entry["exchangeName"],
                                option_symbol,
                                "",
                                "call" if option_type == "callExpDateMap" else "put",
                                date.strftime("%m/%d/%Y"),
                                datetime.now().strftime("%m/%d/%Y"),
                                strike_str,
                                entry["last"],
                                entry["bid"],
                                entry["ask"],
                                entry["totalVolume"],
                                entry["openInterest"],
                                entry["volatility"],
                                entry["delta"],
                                entry["gamma"],
                                entry["theta"],
                                entry["vega"],
                                option_symbol,
                            )
                            self.db_cursor.execute(
                                "INSERT INTO OptionsData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                query_values,
                            )
            self.db_connection.commit()

    def get_option_chain_data(self, symbol: str) -> Dict:
        retries = 60
        while retries:
            response = self.session.get(
                TOS_OPTION_CHAIN_API_URL
                + "?apikey="
                + API_KEY
                + "&symbol="
                + symbol
                + "&strikeCount=512&includeQuotes=TRUE",
                timeout=32,
            )
            data = response.json()
            try:
                if data["status"]:
                    return data
            except KeyError:
                if data["error"]:
                    logging.warning(data["error"])
                    retries = retries - 1
                    time.sleep(2)


def main():
    symbols = [x for x in ALL_SYMBOLS if x not in EXCLUDED_SYMBOLS]
    options_data_downloader = OptionsDataDownloader()
    options_data_downloader.get_and_store_data(symbols)


if __name__ == "__main__":
    main()
