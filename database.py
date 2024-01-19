__version__ = '1.0.0'

import os
import re
import time
import quool
import random
import datetime
import requests
import numpy as np
import pandas as pd
import akshare as ak
from tqdm import tqdm
from pathlib import Path
from quool.request import Request
from urllib.parse import quote
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def ricequant_fetcher(
    user: str,
    password: str,
    driver: str,
    target: str,
    logfile: str = 'update.log',
):
    logger = quool.Logger("ricequant", display_name=True, file=logfile)
    logger.info("=" * 5 + " ricequant fetcher start " + "=" * 5)
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    prefs = {"download.default_directory" : target}
    chrome_options.add_experimental_option("prefs", prefs)

    service = Service(driver)
    driver: webdriver.Chrome = webdriver.Chrome(service=service, options=chrome_options)

    logger.info("visiting https://www.ricequant.com/")
    driver.get("https://www.ricequant.com/")

    time.sleep(5)
    login_button = driver.find_element(By.CLASS_NAME, "user-status")
    login_button.click()
    logger.info("loging in")
    password_login = driver.find_element(By.CSS_SELECTOR, '.el-dialog__body > div > div > ul > li:nth-child(2)')
    password_login.click()
    inputs = driver.find_elements(By.CLASS_NAME, 'el-input__inner')
    for ipt in inputs:
        if '邮箱' in ipt.get_attribute('placeholder'):
            account = ipt
        if '密码' in ipt.get_attribute('placeholder'):
            passwd = ipt
    account.send_keys(user)
    passwd.send_keys(password)
    login_button = driver.find_element(By.CSS_SELECTOR, 'button.el-button.common-button.btn--submit')
    login_button.click()
    logger.info("logged in and redirect to reserch subdomain")

    time.sleep(5)
    driver.get('https://www.ricequant.com/research/')
    time.sleep(5)
    notebook_list = driver.find_element(By.ID, 'notebook_list')
    logger.info("finding `ricequant_fetcher.ipynb`")
    items = notebook_list.find_elements(By.CSS_SELECTOR, '.list_item.row')
    for item in items:
        if 'ricequant_fetcher' in item.text:
            file = item.find_element(By.CSS_SELECTOR, 'a')
            break
    file.click()

    logger.info("wait for some time before redirect to `ricequant_fetcher.ipynb`")
    driver.switch_to.window(driver.window_handles[-1])
    time.sleep(5)
    cell = driver.find_element(By.CSS_SELECTOR, '#menus > div > div > ul > li:nth-child(5)')
    cell.click()
    runall = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '#run_all_cells'))
    )
    logger.info("start running")
    runall.click()
    unfinished = 0
    while True:
        prompts = driver.find_elements(By.CSS_SELECTOR, '.prompt.input_prompt')
        unfinished_cur = 0
        for prompt in prompts:
            if '*' in prompt.text:
                unfinished_cur += 1
        if unfinished_cur != unfinished:
            logger.info(f'tasks left: {unfinished_cur}/{len(prompts)}')
            unfinished = unfinished_cur
        if unfinished == 0:
            break
    logger.info("all tasks are finished")
    driver.close()
    driver.switch_to.window(driver.window_handles[-1])
    driver.refresh()
    time.sleep(5)
    notebook_list = driver.find_element(By.ID, 'notebook_list')
    items = notebook_list.find_elements(By.CSS_SELECTOR, '.list_item.row')
    todaystr = datetime.datetime.today().strftime(r'%Y%m%d')
    logger.info("finding the generated data file")
    for item in items:
        if todaystr in item.text and '.tar.gz' in item.text:
            file = item
            break
    file.click()
    filename = file.text.splitlines()[0]
    filepath_parent = Path(target)
    download_button = driver.find_element(By.CSS_SELECTOR, '.download-button.btn.btn-default.btn-xs')
    download_button.click()
    logger.info(f"downloading {filename}")
    previous_size = -1
    time.sleep(1)
    while True:
        filepath = list(filepath_parent.glob(f'{filename}*'))[0]
        current_size = os.path.getsize(filepath)
        if current_size == previous_size:
            logger.info(f"{filepath} is finished downloading")
            break
        else:
            previous_size = current_size
            time.sleep(2)
    logger.info(f"deleting {filename}")
    time.sleep(5)
    delete_button = driver.find_element(By.CSS_SELECTOR, '.delete-button.btn.btn-default.btn-xs.btn-danger')
    delete_button.click()
    time.sleep(5)
    double_check_delete_button = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '.btn.btn-default.btn-sm.btn-danger'))
    )
    double_check_delete_button.click()
    
    driver.quit()
    logger.info("=" * 5 + " ricequant fetcher stop " + "=" * 5)
    return filepath


class KaiXin(Request):

    __url_base = "http://www.kxdaili.com/dailiip/2/{i}.html"

    def __init__(self):
        super().__init__()
    
    def request(
        self, 
        page_count: int = 10,
        n_jobs: int = 1, 
        backend: str = 'threading'
    ):
        url = [self.__url_base.format(i=i) for i in range(1, page_count + 1)]
        return super().request(url, 'get', n_jobs, backend)

    def callback(self):
        results = []
        etrees = self.etree
        for tree in etrees:
            if tree is None:
                continue
            for tr in tree.xpath("//table[@class='active']//tr")[1:]:
                ip = "".join(tr.xpath('./td[1]/text()')).strip()
                port = "".join(tr.xpath('./td[2]/text()')).strip()
                results.append({
                    "http": "http://" + "%s:%s" % (ip, port),
                    "https": "https://" + "%s:%s" % (ip, port)
                })
        return pd.DataFrame(results)


class KuaiDaili(Request):

    __inha_base = 'https://www.kuaidaili.com/free/inha/{page_index}/'
    __intr_base = 'https://www.kuaidaili.com/free/intr/{page_index}/'

    def __init__(self):
        super().__init__(delay=4)
    
    def request(
        self,
        page_count: int = 20,
        n_jobs: int = 1,
        backend: str = 'threading',
    ):
        url = []
        for page_index in range(1, page_count + 1):
            for pattern in [self.__inha_base, self.__intr_base]:
                url.append(pattern.format(page_index=page_index))
        return super().request(url, 'get', n_jobs, backend)

    def callback(self):
        results = []
        for tree in self.etree:
            if tree is None:
                continue
            proxy_list = tree.xpath('.//table//tr')
            for tr in proxy_list[1:]:
                results.append({
                    "http": "http://" + ':'.join(tr.xpath('./td/text()')[0:2]),
                    "https": "http://" + ':'.join(tr.xpath('./td/text()')[0:2])
                })
        return pd.DataFrame(results)


class Ip3366(Request):

    __type1_base = 'http://www.ip3366.net/free/?stype=1&page={page}' 
    __type2_base = "http://www.ip3366.net/free/?stype=2&page={page}"

    def __init__(self):
        super().__init__()

    def request(
        self,
        page_count: int = 3,
        n_jobs: int = 1,
        backend: str = 'threading',
    ):
        url = []
        for page in range(1, page_count + 1):
            for pattern in [self.__type1_base, self.__type2_base]:
                url.append(pattern.format(page=page))
        return super().request(url, 'get', n_jobs, backend)

    def callback(self):
        results = []
        for text in self.html:
            if text is None:
                continue
            proxies = re.findall(r'<td>(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})</td>[\s\S]*?<td>(\d+)</td>', text)
            for proxy in proxies:
                results.append({"http": "http://" + ":".join(proxy), "https": "http://" + ":".join(proxy)})
        return pd.DataFrame(results)


class Ip98(Request):

    __base_url = "https://www.89ip.cn/index_{page}.html"

    def __init__(self):
        super().__init__()
    
    def request(
        self,
        page_count: int = 20,
        n_jobs: int = 1,
        backend: str = 'threading',
    ):
        url = []
        for page in range(1, page_count + 1):
            url.append(self.__base_url.format(page=page))
        return super().request(url, 'get', n_jobs, backend)
    
    def callback(self):
        results = []
        for text in self.html:
            if text is None:
                continue
            proxies = re.findall(
                r'<td.*?>[\s\S]*?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})[\s\S]*?</td>[\s\S]*?<td.*?>[\s\S]*?(\d+)[\s\S]*?</td>',
                text
            )
            for proxy in proxies:
                results.append({"http": "http://" + ":".join(proxy), "https": "http://" + ":".join(proxy)})
        return pd.DataFrame(results)


class WeiboSearch:
    '''A search crawler engine for weibo
    ====================================
    sample usage:
    >>> result = WeiboSearch.search("keyword")
    '''

    __base = "https://m.weibo.cn/api/container/getIndex?containerid=100103type%3D1%26q%3D{}&page_type=searchall&page={}"
    Logger = quool.Logger("QuoolWeiboSearch")

    @classmethod
    def _get_content(cls, url, headers):

        def _parse(mblog):
            blog = {
                "created_at": mblog["created_at"],
                "text": re.sub(r'<(.*?)>', '', mblog['text']),
                "id": mblog["id"],
                "link": f"https://m.weibo.cn/detail/{mblog['id']}",                    
                "source": mblog["source"],
                "username": mblog["user"]["screen_name"],
                "reposts_count": mblog["reposts_count"],
                "comments_count": mblog["comments_count"],
                "attitudes_count": mblog["attitudes_count"],
                "isLongText": mblog["isLongText"],
            }
            if blog["isLongText"]:
                headers = {
                    "Referer": f"https://m.weibo.cn/detail/{blog['id']}",
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Safari/605.1.15"
                }
                resp = requests.get(f"https://m.weibo.cn/statuses/extend?id={blog['id']}", headers=headers).json()
                blog["full_text"] = resp["data"]["longTextContent"]
            return blog

        # First try to get resources
        res = requests.get(url, headers=headers).json()
        # if it is end
        if res.get("msg"):
            return False

        # if it contains cards
        cards = res["data"]["cards"]
        blogs = []
        for card in cards:
            # find 'mblog' tag and append to result blogs
            mblog = card.get("mblog")
            card_group = card.get("card_group")
            if card.get("mblog"):
                blog = _parse(mblog)
                blogs.append(blog)
            elif card_group:
                for cg in card_group:
                    mblog = cg.get("mblog")
                    if mblog:
                        blog = _parse(mblog)
                        blogs.append(blog)
        return blogs
    
    @classmethod
    def _get_full(cls, keyword: str):
        page = 1
        result = []
        headers = {
            "Referer": f"https://m.weibo.cn/search?containerid=100103type%3D1%26q%3D{quote(keyword, 'utf-8')}",
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
            }
        cls.Logger.info(f"Start in keyword: {keyword}")
        while True:
            cls.Logger.info(f"Getting {keyword}, currently at page: {page} ... ")
            url = cls.__base.format(keyword, page)
            blogs = cls._get_content(url, headers)
            if not blogs:
                break
            result.extend(blogs)
            page += 1
            time.sleep(random.randint(5, 8))
        cls.Logger.info(f"Finished in keyword: {keyword}!")
        return result
    
    @classmethod
    def _get_assigned(cls, keyword: str, pages: int):
        result = []
        cls.Logger.info(f"Start in keyword: {keyword}")
        headers = {
            "Referer": f"https://m.weibo.cn/search?containerid=100103type%3D1%26q%3D{quote(keyword, 'utf-8')}",
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
            }
        for page in tqdm(range(1, pages+1)):
            cls.Logger.info(f"Getting {keyword}, currently at page: {page} ... ")
            url = cls.__base.format(keyword, page)
            blogs = cls._get_content(url, headers)
            result.extend(blogs)
            time.sleep(random.randint(5, 8))
        cls.Logger.info(f"Finished in keyword: {keyword}!")
        return result          
    
    @classmethod
    def search(cls, keyword: str, pages: int = -1):
        """Search for the keyword
        --------------------------
        
        keyword: str, keyword
        pages: int, how many pages you want to get, default -1 to all pages
        """

        keyword = keyword.replace('#', '%23')
        if pages == -1:
            result = cls._get_full(keyword)
        else:
            result = cls._get_assigned(keyword, pages)
        result = pd.DataFrame(result)
        return result


class AkShare:
    """
    AkShare is a class designed to interface with the AkShare API, providing methods to fetch 
    a variety of financial data. It simplifies the process of accessing and retrieving data 
    related to stock markets, ETFs, and other financial instruments.

    Class Attributes:
        - TODAY: A pd.Timestamp object representing today's date.
        - START: A string representing the default start date for fetching historical data.
        - Logger: A Logger object for logging messages.

    Class Methods:
        - market_daily: Retrieves daily market prices for a specific stock.
        - stock_quote: Fetches real-time quotes for stocks in the A-share market.
        - plate_quote: Obtains real-time quotes for industry plates.
        - etf_market_daily: Gets daily market prices for a specific ETF.
        - stock_fund_flow: Retrieves fund flow data for a specific stock.
        - stock_fund_rank: Fetches fund flow rankings for stocks.
        - plate_info: Provides information about stocks within a specific plate.
        - balance_sheet: Fetches balance sheet data for a given stock.
        - profit_sheet: Retrieves profit sheet data for a given stock.
        - cashflow_sheet: Obtains cash flow sheet data for a specified stock.
        - index_weight: Fetches index weight data for a given stock index.

    Usage Example:
    --------------
    # Fetching daily market data for a specific stock
    daily_data = AkShare.market_daily('600000', start='20200101', end='20201231')

    # Obtaining real-time quotes for stocks
    stock_data = AkShare.stock_quote()

    # Getting balance sheet data for a stock
    balance_data = AkShare.balance_sheet('600000')
    """
    TODAY = pd.to_datetime(datetime.datetime.today()).normalize()
    START = '20050101'
    logger = quool.Logger("QuoolAkShare")
    
    @classmethod
    def market_daily(cls, code: str, start: str = None, end: str = None):
        """Get market daily prices for one specific stock
        
        code: str, the code of the stock
        start: str, start date in string format
        end: str, end date in string format
        """
        start = start or cls.START
        end = end or cls.TODAY.strftime('%Y%m%d')

        price = ak.stock_zh_a_hist(symbol=code, start_date=start, end_date=end, adjust='')
        if not price.empty:
            price = price.set_index('日期')
        else:
            return price
        adjprice = ak.stock_zh_a_hist(symbol=code, start_date=start, end_date=end, adjust='hfq')
        if not adjprice.empty:
            adjprice = adjprice.set_index('日期')
        else:
            return adjprice
        adjfactor = adjprice['收盘'] / price['收盘']
        adjfactor.name = 'adjfactor'
        price = pd.concat([price, adjfactor], axis=1)
        price = price.rename(columns = {
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
            "涨跌幅": "pctchange",
            "振幅": "vibration",
            "涨跌额": "change",
            "换手率": "turnover",
        }).astype('f')
        price.index = pd.to_datetime(price.index)
        price.index.name = 'datetime'

        return price

    @classmethod
    def stock_quote(cls, code_only: bool = False):
        """Get the realtime quote amoung the a stock share market

        code_only: bool, decide only return codes on the market
        """
        price = ak.stock_zh_a_spot_em()
        price = price.set_index('代码').drop('序号', axis=1)
        if code_only:
            return price.index.to_list()
        return price

    @classmethod
    def plate_quote(cls, name_only: bool = False):
        data = ak.stock_board_industry_name_em()
        data = data.set_index('板块名称')
        if name_only:
            return data.index.to_list()
        return data

    @classmethod
    def etf_market_daily(cls, code: str, start: str = None, end: str = None):
        start = start or cls.START
        end = end or cls.TODAY.strftime('%Y%m%d')
        price = ak.fund_etf_fund_info_em(code, start, end).set_index('净值日期')
        price.index = pd.to_datetime(price.index)
        return price
    
    @classmethod
    def stock_fund_flow(cls, code: str):
        code, market = code.split('.')
        if market.isdigit():
            code, market = market, code
        market = market.lower()
        funds = ak.stock_individual_fund_flow(stock=code, market=market)
        funds = funds.set_index('日期')
        funds.index = pd.MultiIndex.from_product([[code], 
            pd.to_datetime(funds.index)], names=['日期', '代码'])
        return funds
    
    @classmethod
    def stock_fund_rank(cls):
        datas = []
        for indi in ['今日', '3日', '5日', '10日']:
            datas.append(ak.stock_individual_fund_flow_rank(indicator=indi
                ).drop('序号', axis=1).set_index('代码').rename(columns={'最新价': f'{indi}最新价'}))
        datas = pd.concat(datas, axis=1)
        datas['简称'] = datas.iloc[:, 0]
        datas = datas.drop('名称', axis=1)
        datas = datas.replace('-', None).apply(pd.to_numeric, errors='ignore')
        datas.index = pd.MultiIndex.from_product([[cls.today], datas.index], names=['日期', '代码'])
        return datas
    
    @classmethod
    def plate_info(cls, plate: str):
        data = ak.stock_board_industry_cons_em(symbol=plate).set_index('代码')
        return data

    @classmethod
    def balance_sheet(cls, code):
        try:
            data = ak.stock_balance_sheet_by_report_em(symbol=code)
            if data.empty:
                return None
            data = data.drop([
                'SECURITY_CODE', 'SECURITY_NAME_ABBR', 'ORG_CODE', 'ORG_TYPE', 'REPORT_TYPE',
                'REPORT_DATE_NAME', 'SECURITY_TYPE_CODE', 'UPDATE_DATE', 'CURRENCY', 'LISTING_STATE'
            ], axis=1)
            data = data.replace({None: np.nan})
            data = data.astype('float32', errors='ignore')
            data[['REPORT_DATE', 'NOTICE_DATE']] = data[['REPORT_DATE', 'NOTICE_DATE']].astype('datetime64[ns]')
            data = data.set_index('REPORT_DATE')
            data = data.reindex(pd.date_range(data.index.min(), data.index.max(), freq='q'))
            data['SECUCODE'] = data['SECUCODE'][~data['SECUCODE'].isna()].iloc[0]
            data = data.set_index(['SECUCODE', 'NOTICE_DATE'], append=True)
            data.index.names = ['report_date', 'secucode', 'notice_date']
            data = data.rename(columns=lambda x: x.lower())
            return data
        except:
            cls.Logger.warning(f'{code} get balance sheet failed!, please try again mannually')
            return None

    @classmethod
    def profit_sheet(cls, code):
        try:
            data = ak.stock_profit_sheet_by_report_em(symbol=code)
            if data.empty:
                return None
            data = data.drop([
                'SECURITY_CODE', 'SECURITY_NAME_ABBR', 'ORG_CODE', 'ORG_TYPE', 'REPORT_TYPE',
                'REPORT_DATE_NAME', 'SECURITY_TYPE_CODE', 'UPDATE_DATE', 'CURRENCY'
            ], axis=1)
            data = data.replace({None: np.nan})
            data = data.astype('float32', errors='ignore')
            data[['REPORT_DATE', 'NOTICE_DATE']] = data[['REPORT_DATE', 'NOTICE_DATE']].astype('datetime64[ns]')
            data = data.set_index('REPORT_DATE')
            data = data.reindex(pd.date_range(data.index.min(), data.index.max(), freq='q'))
            data['SECUCODE'] = data['SECUCODE'][~data['SECUCODE'].isna()].iloc[0]
            data = data.set_index(['SECUCODE', 'NOTICE_DATE'], append=True)
            data.index.names = ['report_date', 'secucode', 'notice_date']
            data = data.rename(columns=lambda x: x.lower())
            return data
        except:
            cls.Logger.warning(f'{code} get balance sheet failed!, please try again mannually')
            return None

    @classmethod
    def cashflow_sheet(cls, code):
        try:
            data = ak.stock_cash_flow_sheet_by_report_em(symbol=code)
            if data.empty:
                return None
            data = data.drop([
                'SECURITY_CODE', 'SECURITY_NAME_ABBR', 'ORG_CODE', 'ORG_TYPE', 'REPORT_TYPE',
                'REPORT_DATE_NAME', 'SECURITY_TYPE_CODE', 'UPDATE_DATE', 'CURRENCY'
            ], axis=1)
            data = data.replace({None: np.nan})
            data = data.astype('float32', errors='ignore')
            data[['REPORT_DATE', 'NOTICE_DATE']] = data[['REPORT_DATE', 'NOTICE_DATE']].astype('datetime64[ns]')
            data = data.set_index('REPORT_DATE')
            data = data.reindex(pd.date_range(data.index.min(), data.index.max(), freq='q'))
            data['SECUCODE'] = data['SECUCODE'][~data['SECUCODE'].isna()].iloc[0]
            data = data.set_index(['SECUCODE', 'NOTICE_DATE'], append=True)
            data.index.names = ['report_date', 'secucode', 'notice_date']
            data = data.rename(columns=lambda x: x.lower())
            return data
        except:
            cls.Logger.warning(f'{code} get balance sheet failed!, please try again mannually')
            return None

        
    @classmethod
    def index_weight(cls, code: str):
        data = ak.index_stock_cons_weight_csindex(code)
        return data


class EastMoney:
    """
    The 'Em' class is designed to interface with East Money (东方财富网) for fetching 
    financial data and analysis. It provides methods to access various types of 
    financial information such as stock market movements and expert analyses.

    Class Attributes:
        - headers: Standard headers used for HTTP requests to East Money.

    Class Methods:
        - look_updown: Fetches real-time rise and fall data for a specific stock.

    Usage Example:
    --------------
    # Fetching rise and fall data for a given stock code
    stock_movement = Em.look_updown('600000')

    Notes:
    ------
    This class primarily targets the Chinese stock market and is useful for investors 
    and analysts focusing on this market.
    """

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6 Safari/605.1.15",
        "Referer": "http://guba.eastmoney.com/",
        "Host": "gubacdn.dfcfw.com"
    }

    @classmethod
    def look_updown(cls, code: str):
        today = datetime.datetime.today().date()
        url = f"http://gubacdn.dfcfw.com/LookUpAndDown/{code}.js"
        res = requests.get(url, headers=cls.headers)
        res.raise_for_status()
        res = eval(res.text.strip('var LookUpAndDown=').replace('null', f'"{today}"'))
        data = pd.Series(res['Data'])
        data['code'] = code
        return data


class StockUS:
    """
    The 'StockUS' class is tailored for interacting with the stock.us market API. 
    It provides functionalities to fetch stock prices, index prices, and research 
    reports from the US market.

    Class Attributes:
        - __root: The root URL for the stock.us API.
        - headers: Standard headers for API requests.
        - category: Dictionary mapping category IDs to their descriptions.

    Class Methods:
        - index_price: Fetches historical price data for a specified index.
        - cn_price: Retrieves historical price data for a specific Chinese stock.
        - report_list: Lists research reports based on various criteria.
        - report_search: Searches for research reports based on keywords or other filters.

    Usage Example:
    --------------
    # Fetching historical price data for a US index
    index_data = StockUS.index_price('NASDAQ')

    # Searching for research reports in the US stock market
    reports = StockUS.report_search(keyword='technology', period='1m')

    Notes:
    ------
    This class is particularly useful for users interested in the stock.us api, 
    providing easy access to a wide range of financial data.
    """

    __root = "https://api.stock.us/api/v1/"
    headers = {
        "Host": "api.stock.us",
        "Origin": "https://stock.us",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6 Safari/605.1.15",
        "Accept-Language": "zh-CN",
    }
    category = {
        1: "宏观经济",
        2: "投资策略",
        3: "行业研究",
        4: "晨会早报",
        8: "金工量化",
        9: "债券研究",
        10: "期货研究",
    }
    todaystr = datetime.datetime.today().strftime(r'%Y%m%d')
            
    @classmethod
    def index_price(
        cls, 
        index: str, 
        start: str = None, 
        end: str = None,
    ):
        start = start or '19900101'
        end = end or cls.todaystr
        url = cls.__root + f"index-price?security_code={index}&start={start}&stop={end}"
        res = requests.get(url, headers=cls.headers).json()
        price = pd.DataFrame(res['price'])
        price['date'] = price['date'].astype('datetime64[ns]')
        price = price.set_index('date')
        return price
    
    @classmethod
    def cn_price(
        cls, 
        code: str, 
        start: str = None,
        end: str = None,
    ):
        start = start or '19900101'
        end = end or cls.todaystr
        url = cls.__root + f"cn-price?security_code={code}&start={start}&stop={end}"
        res = requests.get(url, headers=cls.headers).json()
        price = pd.DataFrame(res['price'])
        price['date'] = price['date'].astype('datetime64[ns]')
        price = price.set_index('date')
        return price
    
    @classmethod
    def report_list(
        cls, 
        category: str = 8,
        sub_category: str = 0,
        keyword: str = '', 
        period: str = 'all', 
        org_name: str = '', 
        author: str = '',
        xcf_years: str = '', 
        search_fields: str = 'title',
        page: int = 1, 
        page_size: int = 100
    ):
        '''Get report data in quant block
        ---------------------------------------
        category: str, category to the field, use StockUS.category to see possible choices
        keyword: str, key word to search, default empty string to list recent 100 entries
        period: str, report during this time period
        q: str, search keyword
        org_name: str, search by org_name
        author: str, search by author
        xcf_years: str, search by xcf_years
        search_fields: str, search in fields, support "title", "content", "content_fp"
        page: int, page number
        page_size: int, page size
        '''
        url = cls.__root + 'research/report-list'
        params = (f'?category={category}&dates={period}&q={keyword}&org_name={org_name}'
                  f'&author={author}&xcf_years={xcf_years}&search_fields={search_fields}'
                  f'&page={page}&page_size={page_size}')
        if category != 8:
            params += f'&sub_category={sub_category}'
        headers = {
            "Referer": "https://stock.us/cn/report/quant",
        }
        headers.update(cls.headers)
        url += params
        res = requests.get(url, headers=headers).json()
        data = pd.DataFrame(res['data'])
        data[['pub_date', 'pub_week']] = data[['pub_date', 'pub_week']].astype('datetime64[ns]')
        data.authors = data.authors.map(
            lambda x: ' '.join(list(map(lambda y: y['name'] + ('*' if y['prize'] else ''), x))))
        data = data.set_index('id')
        return data
    
    @classmethod
    def report_search(
        cls, 
        keyword: str = '', 
        period: str = '3m', 
        org_name: str = '', 
        author_name: str = '',
        xcf_years: str = '', 
        search_fields: str = 'title',
        page: int = 1, 
        page_size: int = 100
    ):
        '''Search report in stockus database
        ---------------------------------------
        keyword: str, key word to search, default empty string to list recent 100 entries
        period: str, report during this time period
        org_name: str, search by org_name
        author: str, search by author
        xcf_years: str, search by xcf_years
        search_fields: str, search in fields, support "title", "content", "content_fp"
        page: int, page number
        page_size: int, page size
        '''
        url = cls.__root + 'research/report-search'
        params = (f'?dates={period}&q={keyword}&org_name={org_name}&author_name={author_name}'
                  f'&xcf_years={xcf_years}&search_fields={search_fields}&page={page}'
                  f'&page_size={page_size}')
        url += params
        res = requests.get(url, headers=cls.headers).json()
        data = pd.DataFrame(res['data'])
        data['pub_date'] = data['pub_date'].astype('datetime64[ns]')
        data.authors = data.authors.map(
            lambda x: ' '.join(list(map(lambda y: y['name'] + ('*' if y['prize'] else ''), x)))
            if isinstance(x, list) else '')
        data = data.set_index('id')
        return data


