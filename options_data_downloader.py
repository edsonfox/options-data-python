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
TOS_DOWNLOAD_DIR = "/Users/edsonfox/Documents/Trading/Historical_Options_Data/ToS/"
CBOE_SYMBOLS_URL = "http://markets.cboe.com/us/options/symboldir/equity_index_options/?download=csv"
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
            self.db_handle.options_data.create_index([("dataDate", ASCENDING), ("symbol", ASCENDING)], unique=True)

    def get_and_pickle_data(self, symbols: List, path: str = "") -> List[str]:
        failed_symbols = []
        today_str = datetime.now().strftime("%Y%m%d")
        path += today_str
        try:
            os.mkdir(path)
        except FileExistsError:
            logging.info("%s directory already exists", today_str)
        for symbol in symbols:
            if [i for i in os.listdir(path) if i.startswith(symbol + "_")]:
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
            with open(path + "/" + symbol + "_" + today_str + "_data.pkl", "wb") as p_data:
                pickle.dump(data, p_data)
        return failed_symbols

    def pickle_to_db(self, folder=None):
        self.connect_and_initialize_db()
        folder = datetime.now().strftime("%Y%m%d") if folder is None else folder
        number_of_docs_before = self.db_handle.options_data.estimated_document_count()
        pkls = [i for i in os.listdir(folder) if i.endswith(".pkl")]
        pkls.sort()
        total_contracts = 0
        hod_data_list = []
        for pkl_file in pkls:
            with open(folder + "/" + pkl_file, "rb") as p_data:
                tos_data = pickle.load(p_data)
            total_contracts = total_contracts + tos_data["numberOfContracts"]
            date_str = pkl_file.split("_")[1]
            hod_data = tos_to_hod(tos_data, date_str)
            hod_data_list.append(hod_data)
            try:
                insert_result = self.db_handle.options_data.insert_one(hod_data)
                logging.debug("Inserted %s with id %s", hod_data["symbol"], insert_result.inserted_id)
            except DuplicateKeyError:
                logging.info("Document for %s from %s already in DB", hod_data["symbol"], hod_data["dataDate"])
                continue
        hod_data_to_csv(hod_data_list, folder)
        logging.info("Converted %s contracts from ToS to HoD format", total_contracts)
        number_of_docs_after = self.db_handle.options_data.estimated_document_count()
        logging.info("Inserted %s new documents to DB", number_of_docs_after - number_of_docs_before)

    def csv_folder_to_db(self, folder_prefix, symbols=None, starting_path: str = ""):
        folders = [x for x in os.listdir(starting_path) if x.startswith(folder_prefix)]
        for folder in folders:
            path = starting_path + "/" + folder if starting_path else folder
            files = [x for x in os.listdir(path) if x.startswith("L2_options_")]
            for file in files:
                path = "/".join([starting_path if starting_path else os.getcwd(), folder, file])
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
                data_date = datetime.strptime(inserted_symbols[symbol][0]["DataDate"], "%m/%d/%Y")
                data["dataDate"] = data_date.strftime("%Y%m%d")
                data["chain"] = inserted_symbols[symbol]
                try:
                    insert_result = self.db_handle.options_data.insert_one(data)
                    logging.info(
                        "Inserted %s with id %s", data["symbol"], insert_result.inserted_id,
                    )
                except DuplicateKeyError:
                    logging.info(
                        "Document for %s from %s already exists in DB", data["symbol"], data["dataDate"],
                    )
        number_of_docs_after = self.db_handle.options_data.estimated_document_count()
        logging.info(
            "Inserted %s new dowcuments from %s CSV rows", number_of_docs_after - number_of_docs_before, num_rows,
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
                    + "&includeQuotes=TRUE",
                    timeout=32,
                )
            except (ConnectionError, ReadTimeout, requests.exceptions.ConnectionError) as error:
                try:
                    logging.error("Failed getting option chain for %s: %s", symbol, error)
                except ProtocolError as p_error:
                    try:
                        logging.error("Failed getting option chain for %s: %s", symbol, p_error)
                    except (requests.exceptions.RequestException, requests.exceptions.ConnectionError,) as r_error:
                        logging.error("Failed getting option chain for %s: %s", symbol, r_error)
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
                logging.info("[%s]: %s", symbol, data["error"])
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

    def get_todays_data(self, path: str = ""):
        symbols = self.get_symbols_in_db()
        for try_num in range(2):
            logging.info("Trial number %s", try_num)
            symbols = self.get_and_pickle_data(symbols, path)
            logging.info("Got %s failing symbols: %s", len(symbols), symbols)
        self.get_and_pickle_data(get_cboe_symbols(), path)
        symbols = MANDATORY_SYMBOLS
        for try_num in range(8):
            logging.info("Mandatory symbols: Trial number %s", try_num)
            symbols = self.get_and_pickle_data(symbols, path)
            logging.info("Got %s failing symbols: %s", len(symbols), symbols)
        if symbols:
            logging.error("**************************************************")
            logging.error("COULD NOT GET THESE MANDATORY SYMBOLS: %s", symbols)
            logging.error("**************************************************")


def tos_to_hod(tos_data: dict, date_str: str) -> dict:
    hod_data = {}
    if tos_data["symbol"].startswith("$") and tos_data["symbol"].endswith(".X"):
        symbol = tos_data["symbol"][1:-2]
    else:
        symbol = tos_data["symbol"]
    hod_data["symbol"] = symbol
    hod_data["dataDate"] = date_str
    logging.debug("Found %s contracts in ToS for %s", tos_data["numberOfContracts"], symbol)
    hod_data["chain"] = []
    underlying_price = tos_data["underlying"]["last"] if tos_data["underlying"] else tos_data["underlyingPrice"]
    for option_type in ["callExpDateMap", "putExpDateMap"]:
        for expiration_str, expiration_row in tos_data[option_type].items():
            expiration = expiration_str.split(":")[0].replace("-", "")
            for strike, strike_list in expiration_row.items():
                hod_chain_row = {}
                hod_chain_row["UnderlyingSymbol"] = symbol
                hod_chain_row["UnderlyingPrice"] = str(underlying_price)
                for entry in strike_list:
                    hod_chain_row["Exchange"] = entry["exchangeName"]
                    hod_chain_row["OptionSymbol"] = entry["symbol"]
                    hod_chain_row["OptionExt"] = ""
                    hod_chain_row["Type"] = "call" if option_type == "callExpDateMap" else "put"
                    hod_chain_row["Expiration"] = ("/").join([expiration[4:6], expiration[6:8], expiration[0:4]])
                    hod_chain_row["DataDate"] = ("/").join([date_str[4:6], date_str[6:8], date_str[0:4]])
                    hod_chain_row["Strike"] = strike
                    hod_chain_row["Last"] = str(entry["last"])
                    hod_chain_row["Bid"] = str(entry["bid"])
                    hod_chain_row["Ask"] = str(entry["ask"])
                    hod_chain_row["Volume"] = str(entry["totalVolume"])
                    hod_chain_row["OpenInterest"] = str(entry["openInterest"])
                    try:
                        hod_chain_row["IV"] = str(round(entry["volatility"] / 100, 2))
                    except TypeError:
                        hod_chain_row["IV"] = "NaN"
                    hod_chain_row["Delta"] = str(entry["delta"])
                    hod_chain_row["Gamma"] = str(entry["gamma"])
                    try:
                        hod_chain_row["Theta"] = str(round(entry["theta"] * 100, 2))
                    except TypeError:
                        hod_chain_row["Theta"] = "NaN"
                    hod_chain_row["Vega"] = str(round(entry["vega"] * 100, 2))
                    hod_chain_row["AKA"] = entry["symbol"]
                    hod_data["chain"].append(hod_chain_row)
    return hod_data


def hod_data_to_csv(hod_data: list, date_str: str):
    with open("options_" + date_str + ".csv", "w") as csv_file:
        csv_writer = csv.writer(csv_file)
        for symbol_data in hod_data:
            for row in symbol_data["chain"]:
                csv_writer.writerow(row.values())


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
    while True:
        if datetime.now().weekday() < 5:
            today_str = datetime.now().strftime("%Y%m%d")
            if today_str not in os.listdir(TOS_DOWNLOAD_DIR):
                if datetime.now().hour > 14:
                    options_data_downloader.get_todays_data(TOS_DOWNLOAD_DIR)
                else:
                    logging.info("Waiting until 3pm")
            else:
                logging.info("%s directory already present", today_str)
        else:
            logging.info("No trading today")
        time.sleep(300)


if __name__ == "__main__":
    main()
