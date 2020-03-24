"""Script to download options data from Think or Swim (ToS) API."""

# Standard libraries
import pickle
from datetime import datetime
import logging
import os
import time
from typing import Dict, List
from json.decoder import JSONDecodeError
import csv
import requests
from requests.exceptions import ReadTimeout
from urllib3.exceptions import ProtocolError

# External dependencies
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

# Application-specific imports

# Constants
TOS_OPTION_CHAIN_API_URL = "https://api.tdameritrade.com/v1/marketdata/chains"
CBOE_SYMBOLS_URL = (
    "http://markets.cboe.com/us/options/symboldir/equity_index_options/?download=csv"
)
MANDATORY_SYMBOLS = [
    "A",
    "AAPL",
    "ABBV",
    "ABT",
    "ACN",
    "ADBE",
    "ADM",
    "ADP",
    "AFL",
    "AGN",
    "AIG",
    "ALB",
    "ALL",
    "AMCR",
    "AMGN",
    "AMZN",
    "AOS",
    "APD",
    "ATO",
    "AXP",
    "AZO",
    "BA",
    "BAC",
    "BDX",
    "BEN",
    "BF.B",
    "BIIB",
    "BK",
    "BKNG",
    "BKX",
    "BLK",
    "BMY",
    "BNS",
    "BRK.B",
    "BYND",
    "C",
    "CAH",
    "CAT",
    "CB",
    "CHTR",
    "CINF",
    "CL",
    "CLX",
    "CLDT",
    "CMCSA",
    "CNP",
    "CMG",
    "COF",
    "COP",
    "COST",
    "CSCO",
    "CTAS",
    "CVS",
    "CVX",
    "CZNC",
    "D",
    "DD",
    "DHR",
    "DIA",
    "DIS",
    "DJX",
    "DOV",
    "DOW",
    "DUK",
    "ECL",
    "ED",
    "EEM",
    "EMN",
    "EMR",
    "ESS",
    "EWW",
    "EWZ",
    "EXC",
    "EXPD",
    "EXPE",
    "F",
    "FB",
    "FDX",
    "FRT",
    "FXI",
    "GD",
    "GDX",
    "GE",
    "GEO",
    "GILD",
    "GLD",
    "GM",
    "GOOG",
    "GOOGL",
    "GPC",
    "GS",
    "GWW",
    "HAL",
    "HD",
    "HGX",
    "HON",
    "HRL",
    "HSBC",
    "IBM",
    "INTC",
    "IP",
    "ITW",
    "IWM",
    "IYR",
    "JNJ",
    "JNUG",
    "JPM",
    "KEY",
    "KHC",
    "KMB",
    "KMI",
    "KO",
    "KR",
    "KTB",
    "LEG",
    "LIN",
    "LLY",
    "LMT",
    "LOW",
    "LYB",
    "LYFT",
    "M",
    "MA",
    "MCD",
    "MDLZ",
    "MDT",
    "MET",
    "MKC",
    "MMM",
    "MO",
    "MPC",
    "MRK",
    "MS",
    "MSFT",
    "MYL",
    "NDX",
    "NEE",
    "NFLX",
    "NGG",
    "NKE",
    "NNN",
    "NOV",
    "NUE",
    "NVDA",
    "O",
    "ODP",
    "OEX",
    "OHI",
    "OIH",
    "OMC",
    "ORCL",
    "OSX",
    "OXY",
    "OZK",
    "PBCT",
    "PEAK",
    "PEB",
    "PEP",
    "PFE",
    "PG",
    "PM",
    "PNR",
    "PPG",
    "PPL",
    "PYPL",
    "QCOM",
    "QQQ",
    "RLG",
    "RLV",
    "ROP",
    "ROST",
    "RTN",
    "RUI",
    "RUT",
    "SBUX",
    "SHW",
    "SIXB",
    "SIXI",
    "SIXM",
    "SIXRE",
    "SIXU",
    "SIXV",
    "SIXY",
    "SKT",
    "SLB",
    "SLV",
    "SMH",
    "SO",
    "SOX",
    "SPG",
    "SPGI",
    "SPY",
    "SPX",
    "STAG",
    "SWK",
    "SYY",
    "T",
    "TAN",
    "TGT",
    "TLT",
    "TROW",
    "TSLA",
    "TXN",
    "UBER",
    "UNH",
    "UNP",
    "UPS",
    "USB",
    "USO",
    "UTX",
    "UTY",
    "V",
    "VFC",
    "VIAC",
    "VIACA",
    "VIX",
    "VTR",
    "VZ",
    "WBA",
    "WDC",
    "WELL",
    "WFC",
    "WMT",
    "WPC",
    "WRK",
    "X",
    "XAU",
    "XBI",
    "XDA",
    "XDB",
    "XDC",
    "XDE",
    "XDN",
    "XDS",
    "XDZ",
    "XEO",
    "XLB",
    "XLE",
    "XLP",
    "XLU",
    "XLY",
    "XOM",
    "XOP",
    "XRT",
    "XSP",
]


class OptionsDataDownloader:
    """OptionsDataDownloader downloads data from ToS API and stores it in a DB."""

    def __init__(self):
        self.session = requests.session()
        self.db_handle = None

    def connect_and_initialize_db(self):
        if not self.db_handle:
            client = MongoClient()
            self.db_handle = client.options
            self.db_handle.options_data.create_index(
                [("dataDate", ASCENDING), ("symbol", ASCENDING)], unique=True
            )

    def get_and_pickle_data(self, symbols: List) -> List[str]:
        failed_symbols = []
        today_str = datetime.now().strftime("%Y%m%d")
        try:
            os.mkdir(today_str)
        except FileExistsError:
            logging.info("%s directory already exists", today_str)
        for symbol in symbols:
            if [i for i in os.listdir(today_str) if i.startswith(symbol + "_")]:
                logging.info("%s already present, skipping", symbol)
                continue
            data = self.get_option_chain_from_broker(symbol)
            if data["status"] == "FAILED":
                logging.debug("Trying $%s.X", symbol)
                data = self.get_option_chain_from_broker("$" + symbol + ".X")
            if data["status"] == "FAILED":
                logging.info("%s FAILED!", symbol)
                failed_symbols.append(symbol)
                continue
            with open(
                today_str + "/" + symbol + "_" + today_str + "_data.pkl", "wb"
            ) as p_data:
                pickle.dump(data, p_data)
        return failed_symbols

    def pickle_to_db(self, folder=None):
        self.connect_and_initialize_db()
        folder = datetime.now().strftime("%Y%m%d") if folder is None else folder
        number_of_docs_before = self.db_handle.options_data.estimated_document_count()
        pkls = [i for i in os.listdir(folder) if i.endswith(".pkl")]
        for pkl_file in pkls:
            with open(folder + "/" + pkl_file, "rb") as p_data:
                data = pickle.load(p_data)
            data["dataDate"] = pkl_file.split("_")[1]
            data = replace_dots_in_keys(data)
            try:
                insert_result = self.db_handle.options_data.insert_one(data)
            except DuplicateKeyError:
                logging.info(
                    "Document for %s from %s already exists in DB",
                    data["symbol"],
                    data["dataDate"],
                )
                continue
            logging.debug(
                "Inserted %s with id %s", data["symbol"], insert_result.inserted_id
            )
        number_of_docs_after = self.db_handle.options_data.estimated_document_count()
        logging.info(
            "Inserted %s new documents to DB",
            number_of_docs_after - number_of_docs_before,
        )

    def csv_folder_to_db(self, folder_prefix, symbols=None):
        folders = [x for x in os.listdir() if x.startswith(folder_prefix)]
        for folder in folders:
            files = [x for x in os.listdir(folder) if x.startswith("L2_options_")]
            for file in files:
                path = "/".join([os.getcwd(), folder, file])
                self.csv_to_db(path, symbols)

    def csv_to_db(self, csv_path, symbols=None):
        self.connect_and_initialize_db()
        number_of_docs_before = self.db_handle.options_data.estimated_document_count()
        with open(csv_path) as csv_file:
            logging.info("Now processing %s", csv_path)
            reader = csv.DictReader(csv_file)
            inserted_symbols = {}
            num_rows = 0
            for row in reader:
                current_symbol = row["UnderlyingSymbol"]
                if symbols is None or current_symbol in symbols:
                    num_rows = num_rows + 1
                    try:
                        inserted_symbols[current_symbol].append(row)
                    except KeyError:
                        inserted_symbols[current_symbol] = [row]
            for symbol in inserted_symbols:
                data = {}
                data["symbol"] = symbol
                data_date = datetime.strptime(
                    inserted_symbols[symbol][0]["DataDate"], "%m/%d/%Y"
                )
                data["dataDate"] = data_date.strftime("%Y%m%d")
                data["chain"] = inserted_symbols[symbol]
                try:
                    insert_result = self.db_handle.options_data.insert_one(data)
                    logging.info(
                        "Inserted %s with id %s",
                        data["symbol"],
                        insert_result.inserted_id,
                    )
                except DuplicateKeyError:
                    logging.info(
                        "Document for %s from %s already exists in DB",
                        data["symbol"],
                        data["dataDate"],
                    )
        number_of_docs_after = self.db_handle.options_data.estimated_document_count()
        logging.info(
            "Inserted %s new dowcuments from %s CSV rows",
            number_of_docs_after - number_of_docs_before,
            num_rows,
        )

    def get_option_chain_from_broker(self, symbol: str, retries: int = 60) -> Dict:
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
            except (ConnectionError, ReadTimeout) as error:
                try:
                    logging.error(
                        "Failed getting option chain for %s: %s", symbol, error
                    )
                except ProtocolError as p_error:
                    try:
                        logging.error(
                            "Failed getting option chain for %s: %s", symbol, p_error
                        )
                    except requests.exceptions.RequestException as r_error:
                        logging.error(
                            "Failed getting option chain for %s: %s", symbol, r_error
                        )
                retries = retries - 1
                time.sleep(2)
                continue
            try:
                data = response.json()
            except JSONDecodeError as error:
                logging.error("Failed to get JSON from %s response: %s", symbol, error)
                retries = retries - 1
                time.sleep(2)
                continue
            if "status" in data.keys():
                return data
            if "error" in data.keys():
                logging.info(data["error"])
            else:
                logging.warning("Data has no status or error: %s", data)
            retries = retries - 1
            time.sleep(2)
        return {}

    def get_symbols_in_db(self) -> List[str]:
        self.connect_and_initialize_db()
        symbols_in_db = self.db_handle.options_data.distinct("symbol")
        logging.debug("Found %s symbols in DB: %s", len(symbols_in_db), symbols_in_db)
        return symbols_in_db


def get_cboe_symbols() -> List[str]:
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
    logging.info("Got %s symbols from CBOE", len(symbols))
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
    symbols = options_data_downloader.get_symbols_in_db()
    for try_num in range(2):
        logging.info("Trial number %s", try_num)
        symbols = options_data_downloader.get_and_pickle_data(symbols)
        logging.info("Got %s failing symbols: %s", len(symbols), symbols)
    options_data_downloader.get_and_pickle_data(get_cboe_symbols())
    symbols = MANDATORY_SYMBOLS
    for try_num in range(8):
        logging.info("Mandatory symbols: Trial number %s", try_num)
        symbols = options_data_downloader.get_and_pickle_data(symbols)
        logging.info("Got %s failing symbols: %s", len(symbols), symbols)
    if symbols:
        logging.error("**************************************************")
        logging.error("COULD NOT GET THESE MANDATORY SYMBOLS: %s", symbols)
        logging.error("**************************************************")


if __name__ == "__main__":
    main()
