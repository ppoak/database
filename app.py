import quool
import tarfile
import argparse
import database
import pandas as pd
from pathlib import Path
from functools import partial


CODE_LEVEL = "order_book_id"
DATE_LEVEL = "date"
DATABASE_ROOT = "/home/data/"
PROXYURI = "/home/data/proxy"


TABLE_DICT = {
    "index-weights": partial(
        quool.PanelTable, 
        date_level=DATE_LEVEL, 
        code_level=CODE_LEVEL
    ),
    "industry-info": partial(
        quool.PanelTable, 
        date_level=DATE_LEVEL, 
        code_level=CODE_LEVEL
    ),
    "instruments-info": partial(quool.FrameTable),
    "quotes-day": partial(
        quool.PanelTable, 
        date_level=DATE_LEVEL, 
        code_level=CODE_LEVEL
    ),
    "quotes-min": partial(
        quool.PanelTable, 
        date_level="datetime", 
        code_level=CODE_LEVEL
    ),
    "security-margin": partial(
        quool.PanelTable, 
        date_level="date", 
        code_level=CODE_LEVEL
    ),
    "stock-connect": partial(
        quool.PanelTable, 
        date_level=DATE_LEVEL, 
        code_level=CODE_LEVEL
    ),
    "financial": partial(
        quool.PanelTable, 
        date_level=DATE_LEVEL, 
        code_level=CODE_LEVEL, 
        freq='Y', 
        format='%Y'
    ),
    "dividend-split": partial(
        quool.PanelTable, 
        date_level=DATE_LEVEL, 
        code_level=CODE_LEVEL, 
        freq='Y', 
        format='%Y'
    ),
    "index-quotes-day": partial(
        quool.PanelTable, 
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
    parser.add_argument('--logfile', type=str, default='./update.log', help="path to your logfile")
    args = parser.parse_args()
    return args

def update_data(filename: str, table_base: str, logfile: str = 'debug.log'):
    logger = quool.Logger("UpdateData", stream=False, display_name=True, file=logfile)
    logger.info("=" * 5 + " update data start " + "=" * 5)
    data_path = Path(filename).expanduser().resolve()
    directory = data_path.parent / data_path.stem
    directory.mkdir(exist_ok=True, parents=True)
    if not data_path.is_dir():
        with tarfile.open(data_path, f'r:{data_path.suffix[1:]}') as tar:
            tar.extractall(path=directory)
        data_path.unlink()
    else:
        directory = data_path

    logger.info('-' * 20)
    for file in directory.glob('**/*.parquet'):
        logger.info(f'processing {file}')
        _update_data(file, table_base)

    directory.rmdir()
    logger.info("=" * 5 + " update data stop " + "=" * 5)

def _update_data(filename: str | Path, table_base: str, logfile: str = 'debug.log'):
    filename = Path(filename).expanduser().resolve()
    name = filename.stem.split('_')[0]
    table = TABLE_DICT[name](Path(table_base).joinpath(name))
    df = pd.read_parquet(filename)
    table.update(df)
    filename.unlink()

def update_proxy(proxy_table_path: str, logfile: str = 'debug.log'):
    logger = quool.Logger("UpdateProxy", stream=False, display_name=True, file=logfile)
    logger.info("=" * 5 + " update proxy start " + "=" * 5)
    table = quool.FrameTable(proxy_table_path)
    proxy = table.read()
    logger.info(f'fetching kaixin proxy source')
    kx = database.KaiXin().request(n_jobs=-1).callback()
    logger.info(f'fetching kuaidaili proxy source')
    kdl = database.KuaiDaili().request(n_jobs=-1).callback()
    logger.info(f'fetching ip3366 proxy source')
    ip3366 = database.Ip3366().request(n_jobs=-1).callback()
    logger.info(f'fetching ip98 proxy source')
    ip98 = database.Ip98().request(n_jobs=-1).callback()
    logger.info(f'checking availability or proxies')
    data = pd.concat([proxy, kx, kdl, ip3366, ip98], ignore_index=True)
    
    records = proxy.to_dict(orient='records')
    check_url = "http://httpbin.org/ip"
    valid_index = []
    for i, rec in enumerate(records):
        res = database.Request(proxies=[rec]).request(check_url, n_jobs=-1)
        if (res.responses[0]):
            try: 
                res = res.json()
                if res.get('origin') == rec["http"] or res.get('origin') == rec["https"]:
                    valid_index.append(i)
            except:
                pass
    data = data.loc[valid_index]
    table.update(data)
    logger.info("=" * 5 + " update proxy stop " + "=" * 5)

def backup_data(uri: str | Path, backup: str | Path, logfile: str = "debug.log"):
    logger = quool.Logger("UpdateProxy", stream=False, display_name=True, file=logfile)
    logger.info("=" * 5 + " backup data start " + "=" * 5)
    uri = Path(uri).expanduser().resolve()
    backup = Path(backup).expanduser().resolve()
    backup.mkdir(parents=True, exist_ok=True)
    if not uri.is_dir():
        raise ValueError('uri must be a directory')
    with tarfile.open(str(backup / uri.name) + '.tar.gz', "w:gz") as tar:
        for file in uri.glob('**/*.parquet'):
            tar.add(file)
    logger.info("=" * 5 + " backup data stop " + "=" * 5)

def unbackup_data(backup: str | Path, uribase: str | Path = '/'):
    backup = Path(backup).expanduser().resolve()
    uribase = Path(uribase).expanduser().resolve()
    with tarfile.open(backup, f"r:{backup.suffix.split('.')[-1]}") as tar:
        tar.extractall(path=uribase)


if __name__ == "__main__":
    args = parse_args()
    user, password, driver, target, backup, logfile = (args.user, 
        args.password, args.driver, args.target, args.backup, args.logfile)
    filename = database.ricequant_fetcher(user, password, driver, target, logfile)
    update_data(filename, DATABASE_ROOT, logfile=logfile)
    update_proxy(PROXYURI, logfile=logfile)
    for uri in Path(DATABASE_ROOT).iterdir():
        backup_data(uri, backup)
