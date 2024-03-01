# %%
"""本脚本用于爬虫数据，包括实时市场数据、高频数据等"""
import random
import datetime
import requests
import numpy as np
import pandas as pd
import akshare as ak
from tqdm import tqdm
from pathlib import Path
from copy import deepcopy
from joblib import Parallel, delayed

# %%
# 定义基础数据工具函数、常量
TODAYSTR = datetime.datetime.today().strftime("%Y-%m-%d") 

def format_code(code, format="{code}.{market}", style: str = "wind", upper: bool = False):
    if code.startswith("6"):
        if upper:
            return format.format(code=code, market="SH" if style == "wind" else "XSHG")
        else:
            return format.format(code=code, market="sh" if style == "wind" else "xshg")
    elif code.startswith("3") or code.startswith("0"):
        if upper:
            return format.format(code=code, market="SZ" if style == "wind" else "XSHE")
        else:
            return format.format(code=code, market="sz" if style == "wind" else "xshe")
    else:
        return np.nan

def sub_suffix(code):
    return code[:6]

def proxy_request(url: str, proxies: list = None, **kwargs):
    if not isinstance(proxies, list):
        proxies = [proxies]
    proxies = deepcopy(proxies)
    while len(proxies):
        proxy = proxies.pop(random.randint(0, len(proxies) - 1))
        try:
            return requests.get(url, proxies=proxy, **kwargs)
        except:
            continue
    raise ConnectionError("request failed")

# %%
# 定义爬虫函数，来源akshare
def get_orderbook_data(symbol: str = "sz000001", proxies: list = None) -> pd.DataFrame:
    """获取当天订单簿数据"""
    big_df = pd.DataFrame()
    page = 0
    while True:
        try:
            url = "http://stock.gtimg.cn/data/index.php"
            params = {
                "appn": "detail",
                "action": "data",
                "c": symbol,
                "p": page,
            }
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1788.0  uacq'}
            r = proxy_request(url, proxies, headers=headers, params=params)
            text_data = r.text
            temp_df = (
                pd.DataFrame(eval(text_data[text_data.find("[") :])[1].split("|"))
                .iloc[:, 0]
                .str.split("/", expand=True)
            )
            page += 1
            big_df = pd.concat([big_df, temp_df], ignore_index=True)
        except:
            break
    
    if not big_df.empty:
        big_df = big_df.iloc[:, 1:].copy()
        big_df.columns = ["datetime", "price", "price_chg", "volume", "amount", "direction"]
        big_df["datetime"] = TODAYSTR + " " + big_df["datetime"]
        big_df["datetime"] = pd.to_datetime(big_df["datetime"])
        big_df = big_df.set_index('datetime')
        big_df = big_df.astype(
            {
                "price": float,
                "price_chg": float,
                "volume": int,
                "amount": int,
                "direction": str,
            }
        )
    
    return big_df

def get_spot_data(proxies: list = None) -> pd.DataFrame:
    """获取市场实时数据"""
    url = "http://82.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "50000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152",
        "_": "1623833739532",
    }
    r = proxy_request(url, proxies, params=params)
    data_json = r.json()
    if not data_json["data"]["diff"]:
        return pd.DataFrame()
    temp_df = pd.DataFrame(data_json["data"]["diff"])
    temp_df.columns = [
        "_",
        "latest_price",
        "change_rate",
        "change_amount",
        "volume",
        "turnover",
        "amplitude",
        "turnover_rate",
        "pe_ratio_dynamic",
        "volume_ratio",
        "five_minute_change",
        "code",
        "_",
        "name",
        "highest",
        "lowest",
        "open",
        "previous_close",
        "market_cap",
        "circulating_market_cap",
        "speed_of_increase",
        "pb_ratio",
        "sixty_day_change_rate",
        "year_to_date_change_rate",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
    ]
    
    temp_df["code"] = temp_df["code"].map(lambda x: format_code(x, format='{code}.{market}', style='ricequant', upper=True))
    temp_df = temp_df.dropna(subset=["code"]).set_index("code")
    temp_df = temp_df.drop(["-", "_"], axis=1)
    for col in temp_df.columns:
        if col != 'name':
            temp_df[col] = pd.to_numeric(temp_df[col], errors='coerce')
    return temp_df

# %%
# 获取本地代理池
proxies = pd.read_parquet("/home/data/proxy", columns=["http"]).to_dict('records')

# %%
# 获取实时数据
quote = get_spot_data(proxies)

# %%
path = Path("/home/data/orderbook")
path.mkdir(parents=True, exist_ok=True)
code = quote.index.str.slice(0, 6).map(lambda x: format_code(x, format='{market}{code}', upper=False))

def _get(c):
    df = get_orderbook_data(c, proxies=proxies)
    pathc = path / c
    pathc.mkdir(parents=True, exist_ok=True)
    df.to_parquet(pathc / (TODAYSTR + '.parquet'))
    return df

Parallel(n_jobs=-1, backend='threading')(delayed(_get)(c) for c in tqdm(code))

# %%
