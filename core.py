import os
import time
import tarfile
import datetime
import argparse
import collector
import pandas as pd
from pathlib import Path
from functools import partial
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from quool import Logger
from quool.table import PanelTable, FrameTable


CODE_LEVEL = "order_book_id"
DATE_LEVEL = "date"


TABLE_DICT = {
    "index-weights": partial(
        PanelTable, 
        date_level=DATE_LEVEL, 
        code_level=CODE_LEVEL
    ),
    "industry-info": partial(
        PanelTable, 
        date_level=DATE_LEVEL, 
        code_level=CODE_LEVEL
    ),
    "instruments-info": partial(FrameTable),
    "quotes-day": partial(
        PanelTable, 
        date_level=DATE_LEVEL, 
        code_level=CODE_LEVEL
    ),
    "quotes-min": partial(
        PanelTable, 
        date_level="datetime", 
        code_level=CODE_LEVEL
    ),
    "security-margin": partial(
        PanelTable, 
        date_level="date", 
        code_level=CODE_LEVEL
    ),
    "stock-connect": partial(
        PanelTable, 
        date_level=DATE_LEVEL, 
        code_level=CODE_LEVEL
    ),
    "financial": partial(
        PanelTable, 
        date_level=DATE_LEVEL, 
        code_level=CODE_LEVEL, 
        freq='Y', 
        format='%Y'
    ),
    "dividend-split": partial(
        PanelTable, 
        date_level=DATE_LEVEL, 
        code_level=CODE_LEVEL, 
        freq='Y', 
        format='%Y'
    ),
    "index-quotes-day": partial(
        PanelTable, 
        date_level=DATE_LEVEL, 
        code_level=CODE_LEVEL
    ),
}


def parse_args():
    parser = argparse.ArgumentParser("RiceQuant Automate Updater")
    parser.add_argument('--user', type=str, required=True, help="ricequant user name")
    parser.add_argument('--password', type=str, required=True, help="ricequant password")
    parser.add_argument('--driver', type=str, default="./chromedriver", help="path to your chromedriver")
    parser.add_argument('--target', type=str, default='.', help="path to your target directory")
    parser.add_argument('--backup', type=str, default='.', help="path to your backup directory")
    args = parser.parse_args()
    return args

def fetch_data(
    user: str,
    password: str,
    driver: str,
    target: str,
):
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
    return filepath

def update_data(filename: str, table_base: str):
    data_path = Path(filename).expanduser().resolve()
    directory = data_path.parent / data_path.stem
    directory.mkdir(exist_ok=True, parents=True)
    if not data_path.is_dir():
        with tarfile.open(data_path, f'r:{data_path.suffix[1:]}') as tar:
            tar.extractall(path=directory)
        data_path.unlink()

    logger.info('-' * 20)
    for file in directory.glob('**/*.parquet'):
        logger.info(f'processing {file}')
        _save_data(file, table_base)

    directory.rmdir()

def _save_data(filename: str | Path, table_base: str):
    filename = Path(filename).expanduser().resolve()
    name = filename.stem.split('_')[0]
    table = TABLE_DICT[name](Path(table_base).joinpath(name))
    df = pd.read_parquet(filename)
    table.update(df)
    filename.unlink()

def update_proxy(proxy_table_path: str, logger: Logger):
    table = FrameTable(proxy_table_path)
    proxy = table.read()
    logger.info(f'fetching kaixin proxy source')
    kx = collector.KaiXin().request(n_jobs=-1).callback()
    logger.info(f'fetching kuaidaili proxy source')
    kdl = collector.KuaiDaili().request(n_jobs=-1).callback()
    logger.info(f'fetching ip3366 proxy source')
    ip3366 = collector.Ip3366().request(n_jobs=-1).callback()
    logger.info(f'fetching ip98 proxy source')
    ip98 = collector.Ip98().request(n_jobs=-1).callback()
    logger.info(f'checking availability or proxies')
    data = pd.concat([proxy, kx, kdl, ip3366, ip98], ignore_index=True)
    
    records = proxy.to_dict(orient='records')
    check_url = "http://httpbin.org/ip"
    valid_index = []
    for i, rec in enumerate(records):
        res = collector.Request(proxies=[rec]).request(check_url, n_jobs=-1)
        if (res.responses[0]):
            try: 
                res = res.json()
                if res.get('origin') == rec["http"] or res.get('origin') == rec["https"]:
                    valid_index.append(i)
            except:
                pass
    data = data.loc[valid_index]
    table.update(data)

def backup_data(uri: str | Path, backup: str | Path):
    uri = Path(uri).expanduser().resolve()
    backup = Path(backup).expanduser().resolve()
    backup.mkdir(parents=True, exist_ok=True)
    if not uri.is_dir():
        raise ValueError('uri must be a directory')
    with tarfile.open(str(backup / uri.name) + '.tar.gz', "w:gz") as tar:
        for file in uri.glob('**/*.parquet'):
            tar.add(file)

def unbackup_data(backup: str | Path, uribase: str | Path = '/'):
    backup = Path(backup).expanduser().resolve()
    uribase = Path(uribase).expanduser().resolve()
    with tarfile.open(backup, f"r:{backup.suffix.split('.')[-1]}") as tar:
        tar.extractall(path=uribase)


if __name__ == "__main__":
    logger = Logger(file="update.log", stream=False)
    args = parse_args()
    logger.info("-" * 5 + ' fetching data from ricequant ' + "-" * 5)
    user, password, driver, target, backup = args.user, args.password, args.driver, args.target, args.backup
    filename = fetch_data(user, password, driver, target)
    logger.info("-" * 5 + ' updating data ' + "-" * 5)
    update_data(filename, "/home/data")
    logger.info("-" * 5 + ' update proxy ' + "-" * 5)
    update_proxy('/home/data/proxy', logger)
    logger.info("-" * 5 + ' finished update ' + "-" * 5)
    for uri in Path('/home/data').iterdir():
        backup_data(uri, backup)
    logger.info("-" * 5 + ' finished backup ' + "-" * 5)
