import sys
import pandas as pd
import sqlalchemy
import pandasquant as pq
from .staff import *


local_market_daily_config = dict(
    loader = LocalLoader,
    path = pq.Cache().get('local_market_daily_path'),
    table = "market_daily",
    database = sqlalchemy.engine.create_engine(pq.Cache().get('local')),
    addindex = {
        "idx_market_daily_date": "date",
        "idx_market_daily_order_book_id": "order_book_id",
    },
)

local_index_market_daily_config = dict(
    loader = LocalLoader,
    path = pq.Cache().get('local_index_market_daily_path'),
    table = "index_market_daily",
    database = sqlalchemy.engine.create_engine(pq.Cache().get('local')),
    addindex = {
        "idx_index_market_daily_date": "date",
        "idx_index_market_daily_order_book_id": "order_book_id",
    },
)

local_derivative_indicator_config = dict(
    loader = LocalLoader,
    path = pq.Cache().get('local_derivative_indicator_path'),
    table = "derivative_indicator",
    database = pq.Cache().get('local'),
    addindex = {
        "idx_derivative_indicator_date": "date",
        "idx_derivative_indicator_order_book_id": "order_book_id",
    },
)

tushare_market_daily_config = dict(
    loader = TuShareLoader,
    table = 'market_daily',
    database = sqlalchemy.engine.create_engine(pq.Cache().get('tushare')),
    addindex = {
        "idx_derivative_indicator_trade_date": "trade_date",
        "idx_derivative_indicator_ts_code": "ts_code",
    },
    func = pq.TuShare(pq.Cache().get('token')).market_daily,
    args = zip(*[pd.date_range('20121122', '20220706', freq=pq.CBD)] * 2),
)

configs = dict(
    local_market_daily_config = local_market_daily_config,
    local_index_market_daily_config = local_index_market_daily_config,
    local_derivative_indicator_config = local_derivative_indicator_config,
    tushare_market_daily_config = tushare_market_daily_config,
)

if __name__ == "__main__":
    config_name = sys.argv[1]
    config = configs[config_name]
    loader = config['loader'](config)
    loader()
