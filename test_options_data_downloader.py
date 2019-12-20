"""Tests for OptionsDataDownloader module."""

# Standard libraries
import unittest
from unittest import mock
import os
from requests import Session

# External dependencies

# Application-specific imports
from options_data_downloader import (
    OptionsDataDownloader,
    TOS_OPTION_CHAIN_API_URL,
    replace_dots_in_keys,
)


def mocked_session_get(*args, **kwargs):  # pylint: disable=W0613
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

        def status(self):
            return self.status_code

    if "NO_STATUS_NO_ERROR" in args[0]:
        return MockResponse({"weird": "dict"}, 200)
    if args[0].startswith(TOS_OPTION_CHAIN_API_URL):
        return MockResponse({"status": "PASSED"}, 200)
    return MockResponse(None, 404)


class TestOptionsDataDownloader(unittest.TestCase):
    @mock.patch.object(Session, "get", side_effect=mocked_session_get)
    @mock.patch.dict(os.environ, {"TOS_API_KEY": "dUmmYkEy"})
    def test_fetch(self, mock_get):
        downloader = OptionsDataDownloader()
        json_data = downloader.get_option_chain_from_broker("TSLA")
        self.assertEqual(json_data, {"status": "PASSED"})
        self.assertEqual(len(mock_get.call_args_list), 1)

    @mock.patch.object(Session, "get", side_effect=mocked_session_get)
    @mock.patch.dict(os.environ, {"TOS_API_KEY": "dUmmYkEy"})
    def test_get_option_chain_from_broker_with_no_status_or_error(self, mock_get):
        with self.assertLogs() as logs:
            downloader = OptionsDataDownloader()
            json_data = downloader.get_option_chain_from_broker(
                "NO_STATUS_NO_ERROR", retries=1
            )
        self.assertEqual(
            logs.output, ["WARNING:root:Data has no status or error: {'weird': 'dict'}"]
        )
        self.assertEqual(json_data, {})
        self.assertEqual(len(mock_get.call_args_list), 1)

    def test_replace_dots_in_keys(self):
        dict_1 = {"key.1": "value1"}
        expt_1 = {"key,1": "value1"}
        self.assertEqual(replace_dots_in_keys(dict_1), expt_1)
        dict_2 = {"key2": "value2"}
        expt_2 = {"key2": "value2"}
        self.assertEqual(replace_dots_in_keys(dict_2), expt_2)
        dict_3 = {"key3": {"sub.3": "value3"}}
        expt_3 = {"key3": {"sub,3": "value3"}}
        self.assertEqual(replace_dots_in_keys(dict_3), expt_3)
        dict_4 = {"key4": {"sub4": "value4"}}
        expt_4 = {"key4": {"sub4": "value4"}}
        self.assertEqual(replace_dots_in_keys(dict_4), expt_4)
        dict_5 = {
            "symbol": "BAK",
            "status": "SUCCESS",
            "underlying": {
                "symbol": "BAK",
                "description": "Braskem SA ADR",
                "change": -0.17,
                "percentChange": -1.21,
                "close": 14.08,
                "quoteTime": 1576616520375,
                "tradeTime": 1576625400005,
                "bid": 11.33,
                "ask": 14.29,
                "last": 13.91,
                "mark": 13.91,
                "markChange": -0.17,
                "markPercentChange": -1.21,
                "bidSize": 300,
                "askSize": 100,
                "highPrice": 14.11,
                "lowPrice": 13.89,
                "openPrice": 14.06,
                "totalVolume": 180477,
                "exchangeName": "NYS",
                "fiftyTwoWeekHigh": 15.33,
                "fiftyTwoWeekLow": 12.88,
                "delayed": True,
            },
            "strategy": "SINGLE",
            "interval": 0.0,
            "isDelayed": True,
            "isIndex": False,
            "interestRate": 2.42788,
            "underlyingPrice": 7.145,
            "volatility": 29.0,
            "daysToExpiration": 0.0,
            "numberOfContracts": 2,
            "callExpDateMap": {
                "2019-12-20:3": {
                    "35.0": [
                        {
                            "putCall": "CALL",
                            "symbol": "BAK_122019C35",
                            "description": "BAK Dec 20 2019 35 Call",
                            "exchangeName": "OPR",
                            "bid": 0.0,
                            "ask": 5.0,
                            "last": 0.0,
                            "mark": 2.5,
                            "bidSize": 0,
                            "askSize": 0,
                            "bidAskSize": "0X0",
                            "lastSize": 0,
                            "highPrice": 0.0,
                            "lowPrice": 0.0,
                            "openPrice": 0.0,
                            "closePrice": 0.0,
                            "totalVolume": 0,
                            "tradeDate": None,
                            "tradeTimeInLong": 0,
                            "quoteTimeInLong": 1576593000463,
                            "netChange": 0.0,
                            "volatility": 1000.0,
                            "delta": 0.16,
                            "gamma": 0.032,
                            "theta": -0.227,
                            "vega": 0.002,
                            "rho": 0.0,
                            "openInterest": 0,
                            "timeValue": 2.5,
                            "theoreticalOptionValue": 0.421,
                            "theoreticalVolatility": 29.0,
                            "optionDeliverablesList": None,
                            "strikePrice": 35.0,
                            "expirationDate": 1576893600000,
                            "daysToExpiration": 3,
                            "expirationType": "R",
                            "lastTradingDay": 1576818000000,
                            "multiplier": 100.0,
                            "settlementType": " ",
                            "deliverableNote": "",
                            "isIndexOption": None,
                            "percentChange": 0.0,
                            "markChange": 2.5,
                            "markPercentChange": 833233.29,
                            "nonStandard": False,
                            "mini": False,
                            "inTheMoney": False,
                        }
                    ]
                }
            },
            "putExpDateMap": {
                "2019-12-20:3": {
                    "35.0": [
                        {
                            "putCall": "PUT",
                            "symbol": "BAK_122019P35",
                            "description": "BAK Dec 20 2019 35 Put",
                            "exchangeName": "OPR",
                            "bid": 18.5,
                            "ask": 23.5,
                            "last": 0.0,
                            "mark": 21.0,
                            "bidSize": 0,
                            "askSize": 0,
                            "bidAskSize": "0X0",
                            "lastSize": 0,
                            "highPrice": 0.0,
                            "lowPrice": 0.0,
                            "openPrice": 0.0,
                            "closePrice": 20.92,
                            "totalVolume": 0,
                            "tradeDate": None,
                            "tradeTimeInLong": 0,
                            "quoteTimeInLong": 1576593000463,
                            "netChange": 0.0,
                            "volatility": 5.0,
                            "delta": -1.0,
                            "gamma": 0.0,
                            "theta": 0.0,
                            "vega": 0.034,
                            "rho": 0.0,
                            "openInterest": 0,
                            "timeValue": -0.09,
                            "theoreticalOptionValue": 27.855,
                            "theoreticalVolatility": 29.0,
                            "optionDeliverablesList": None,
                            "strikePrice": 35.0,
                            "expirationDate": 1576893600000,
                            "daysToExpiration": 3,
                            "expirationType": "R",
                            "lastTradingDay": 1576818000000,
                            "multiplier": 100.0,
                            "settlementType": " ",
                            "deliverableNote": "",
                            "isIndexOption": None,
                            "percentChange": 0.0,
                            "markChange": 0.08,
                            "markPercentChange": 0.38,
                            "nonStandard": False,
                            "mini": False,
                            "inTheMoney": True,
                        }
                    ]
                }
            },
        }
        expt_5 = {
            "symbol": "BAK",
            "status": "SUCCESS",
            "underlying": {
                "symbol": "BAK",
                "description": "Braskem SA ADR",
                "change": -0.17,
                "percentChange": -1.21,
                "close": 14.08,
                "quoteTime": 1576616520375,
                "tradeTime": 1576625400005,
                "bid": 11.33,
                "ask": 14.29,
                "last": 13.91,
                "mark": 13.91,
                "markChange": -0.17,
                "markPercentChange": -1.21,
                "bidSize": 300,
                "askSize": 100,
                "highPrice": 14.11,
                "lowPrice": 13.89,
                "openPrice": 14.06,
                "totalVolume": 180477,
                "exchangeName": "NYS",
                "fiftyTwoWeekHigh": 15.33,
                "fiftyTwoWeekLow": 12.88,
                "delayed": True,
            },
            "strategy": "SINGLE",
            "interval": 0.0,
            "isDelayed": True,
            "isIndex": False,
            "interestRate": 2.42788,
            "underlyingPrice": 7.145,
            "volatility": 29.0,
            "daysToExpiration": 0.0,
            "numberOfContracts": 2,
            "callExpDateMap": {
                "2019-12-20:3": {
                    "35,0": [
                        {
                            "putCall": "CALL",
                            "symbol": "BAK_122019C35",
                            "description": "BAK Dec 20 2019 35 Call",
                            "exchangeName": "OPR",
                            "bid": 0.0,
                            "ask": 5.0,
                            "last": 0.0,
                            "mark": 2.5,
                            "bidSize": 0,
                            "askSize": 0,
                            "bidAskSize": "0X0",
                            "lastSize": 0,
                            "highPrice": 0.0,
                            "lowPrice": 0.0,
                            "openPrice": 0.0,
                            "closePrice": 0.0,
                            "totalVolume": 0,
                            "tradeDate": None,
                            "tradeTimeInLong": 0,
                            "quoteTimeInLong": 1576593000463,
                            "netChange": 0.0,
                            "volatility": 1000.0,
                            "delta": 0.16,
                            "gamma": 0.032,
                            "theta": -0.227,
                            "vega": 0.002,
                            "rho": 0.0,
                            "openInterest": 0,
                            "timeValue": 2.5,
                            "theoreticalOptionValue": 0.421,
                            "theoreticalVolatility": 29.0,
                            "optionDeliverablesList": None,
                            "strikePrice": 35.0,
                            "expirationDate": 1576893600000,
                            "daysToExpiration": 3,
                            "expirationType": "R",
                            "lastTradingDay": 1576818000000,
                            "multiplier": 100.0,
                            "settlementType": " ",
                            "deliverableNote": "",
                            "isIndexOption": None,
                            "percentChange": 0.0,
                            "markChange": 2.5,
                            "markPercentChange": 833233.29,
                            "nonStandard": False,
                            "mini": False,
                            "inTheMoney": False,
                        }
                    ]
                }
            },
            "putExpDateMap": {
                "2019-12-20:3": {
                    "35,0": [
                        {
                            "putCall": "PUT",
                            "symbol": "BAK_122019P35",
                            "description": "BAK Dec 20 2019 35 Put",
                            "exchangeName": "OPR",
                            "bid": 18.5,
                            "ask": 23.5,
                            "last": 0.0,
                            "mark": 21.0,
                            "bidSize": 0,
                            "askSize": 0,
                            "bidAskSize": "0X0",
                            "lastSize": 0,
                            "highPrice": 0.0,
                            "lowPrice": 0.0,
                            "openPrice": 0.0,
                            "closePrice": 20.92,
                            "totalVolume": 0,
                            "tradeDate": None,
                            "tradeTimeInLong": 0,
                            "quoteTimeInLong": 1576593000463,
                            "netChange": 0.0,
                            "volatility": 5.0,
                            "delta": -1.0,
                            "gamma": 0.0,
                            "theta": 0.0,
                            "vega": 0.034,
                            "rho": 0.0,
                            "openInterest": 0,
                            "timeValue": -0.09,
                            "theoreticalOptionValue": 27.855,
                            "theoreticalVolatility": 29.0,
                            "optionDeliverablesList": None,
                            "strikePrice": 35.0,
                            "expirationDate": 1576893600000,
                            "daysToExpiration": 3,
                            "expirationType": "R",
                            "lastTradingDay": 1576818000000,
                            "multiplier": 100.0,
                            "settlementType": " ",
                            "deliverableNote": "",
                            "isIndexOption": None,
                            "percentChange": 0.0,
                            "markChange": 0.08,
                            "markPercentChange": 0.38,
                            "nonStandard": False,
                            "mini": False,
                            "inTheMoney": True,
                        }
                    ]
                }
            },
        }
        self.assertEqual(replace_dots_in_keys(dict_5), expt_5)


if __name__ == "__main__":
    unittest.main()
