# Options Data Downloader

Simple script to collect stock option contracts market data from the Think or Swim (ToS) option chain API 

### Usage

In a Python 3 environment simply type

```
python options_data_downloader.py
```

The script will iterate over the provided stock symbols and retrieve their option chains in JSON format from ToS. Then it will pickle the JSON files and add their data to mongoDB
