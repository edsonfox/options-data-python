import requests
import pprint
import pickle
from datetime import datetime, date
import sqlite3
from sqlite3 import Error
import logging
from symbols import all_symbols, excluded_symbols
from tokens import access_token, api_key
import os
import time

pp = pprint.PrettyPrinter(indent=4)
session = requests.session()
headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "en;q=1, fr;q=0.9, de;q=0.8, ja;q=0.7, nl;q=0.6, it;q=0.5",
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "X-Robinhood-API-Version": "1.0.0",
            "Connection": "keep-alive",
            "User-Agent": "Robinhood/823 (iPhone; iOS 7.1.2; Scale/2.00)",
            "Authorization": "Bearer " + access_token
            }

db_url = "/Users/edsonfox/GitHub/edsonfox/options-data-python/options-data.sqlite"
db_connection = sqlite3.connect(db_url)
db_cursor = db_connection.cursor()

option_chain_api = "https://api.tdameritrade.com/v1/marketdata/chains"

def _does_row_exist(query_values: tuple):
    logging.debug("Checking if row with these values already exists in the DB: %s", query_values)
    query_str = "SELECT * FROM OptionsData WHERE"
    query_str += " UnderlyingSymbol=?"
    query_str += " AND UnderlyingPrice=?"
    query_str += " AND Exchange=?"
    query_str += " AND OptionSymbol=?"
    query_str += " AND OptionExt=?"
    query_str += " AND Type=?"
    query_str += " AND Expiration=?"
    query_str += " AND DataDate=?"
    query_str += " AND Strike=?"
    query_str += " AND Last=?"
    query_str += " AND Bid=?"
    query_str += " AND Ask=?"
    query_str += " AND Volume=?"
    query_str += " AND OpenInterest=?"
    query_str += " AND IV=?"
    query_str += " AND Delta=?"
    query_str += " AND Gamma=?"
    query_str += " AND Theta=?"
    query_str += " AND Vega=?"
    query_str += " AND AKA=?"
    db_cursor.execute(query_str, query_values)
    return bool(db_cursor.fetchall())

failed_symbols = []

def get_data(symbol):
    need_to_wait = True
    while(need_to_wait):
        res = session.get(option_chain_api + "?apikey=" + api_key + "&symbol="+symbol+"&strikeCount=512&includeQuotes=TRUE", timeout=32)
        data = res.json()
        try:
            if data["status"]:
                return data
        except KeyError:
            if data["error"]:
                print(data["error"])
                time.sleep(2)

symbols = [x for x in all_symbols if x not in excluded_symbols]

for symbol in symbols:
    if [i for i in os.listdir(".") if i.startswith(symbol + "_")]:
        print(symbol + " already present, skipping")
        continue
    data = get_data(symbol)
    
    if data["status"] == "FAILED":
        print(symbol + " FAILED!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        failed_symbols.append(symbol)
        continue

    underlying_symbol = data['symbol']
    underlying_price = data['underlying']["mark"]
    quote_time = datetime.fromtimestamp(data['underlying']["quoteTime"]/1000)
    data_date = quote_time.strftime("%m/%d/%Y")  # MM/DD/YYYY
    # these are for stock quotes table
    quote_date = data_date
    open_price = data["underlying"]["openPrice"]
    high_price = data["underlying"]["highPrice"]
    low_price = data["underlying"]["lowPrice"]
    close_price = data["underlying"]["close"]
    stock_volume = data["underlying"]["totalVolume"]
    adjusted_close = close_price  # need to find another source?

    data_date_ymd = quote_time.strftime("%Y%m%d")
    with open(symbol + "_" + data_date_ymd + '_data.pkl', 'wb') as p_data:
        pickle.dump(data, p_data)
    # with open('tsla_data.pkl', 'rb') as p_data:
    #     data = pickle.load(p_data)

    for data_key in ("putExpDateMap", "callExpDateMap"):
        for date in data[data_key]:
            date_list = date.split(":",1)[0].split("-")
            expiration = "/".join([date_list[1], date_list[2], date_list[0]])  # MM/DD/YYYY
            year = date_list[0][2:]
            month = date_list[1]
            day = date_list[2]
            expiration_string = year + month + day
            option_type = "call" if data_key == "callExpDateMap" else "put"
            for strike_str in data[data_key][date]:
                strike = float(strike_str)
                strike_string = "{:09.3f}".format(float(strike_str)).replace(".", "")
                for entry in data[data_key][date][strike_str]:
                    exchange_name = entry["exchangeName"]
                    type_char = "C" if data_key == "callExpDateMap" else "P"
                    option_symbol = underlying_symbol + expiration_string + type_char + strike_string  # NVDA190301C00085000
                    option_ext = ""
                    last = entry["last"]
                    bid = entry["bid"]
                    ask = entry["ask"]
                    volume = entry["totalVolume"]
                    open_interest = entry["openInterest"]
                    iv = entry["volatility"]  # this is 'Implied Volatility" column in ToS in %
                    delta = entry["delta"]
                    gamma = entry["gamma"]
                    theta = entry["theta"]
                    vega = entry["vega"]
                    aka = option_symbol  # need to check this if have time
                    query_values = (underlying_symbol, underlying_price, exchange_name, option_symbol, option_ext,
                        option_type, expiration, data_date, strike, last, bid, ask, volume, open_interest, iv,
                        delta, gamma, theta, vega, aka)
                    # if _does_row_exist(query_values):
                    #     logging.debug("Row with these values already exists in the DB: %s", query_values)
                    # else:
                    db_cursor.execute("INSERT INTO OptionsData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", query_values)
    db_connection.commit()

    # print(f"underlying_symbol {underlying_symbol}")
    # print(f"underlying_price {underlying_price}")
    # print(f"exchange_name {exchange_name}")
    # print(f"option_symbol {option_symbol}")
    # print(f"option_ext {option_ext}")
    # print(f"option_type {option_type}")
    # print(f"expiration {expiration}")
    # print(f"data_date {data_date}")
    # print(f"strike {strike}")
    # print(f"last {last}")
    # print(f"bid {bid}")
    # print(f"ask {ask}")
    # print(f"volume {volume}")
    # print(f"open_interest {open_interest}")
    # print(f"iv {iv}")
    # print(f"delta {delta}")
    # print(f"gamma {gamma}")
    # print(f"theta {theta}")
    # print(f"vega {vega}")
    # print(f"aka {aka}")