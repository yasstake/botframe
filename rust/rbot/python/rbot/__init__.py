from .rbot import *
import pandas as pd


if hasattr(rbot, "__all__"):
    __all__ = rbot.__all__


def decode_order_side(bs):
    if bs == 0:
        return "Sell"
    elif bs == 1:
        return "Buy"
    else:
        return "ERROR"


def decode_liquid(liq):
    if liq == 0:
        return False
    else:
        return True


def trades_to_df(array):
    df = pd.DataFrame(
        array, columns=["timestamp", "price", "size", "side", "liquid"])
    df['timestamp'] = pd.to_datetime(
        (df["timestamp"]), utc=True, unit='us')
    df = df.set_index('timestamp')

    df['side'] = df['side'].map(decode_order_side)
    df['liquid'] = df['liquid'].map(decode_liquid)

    return df


def ohlcvv_to_df(array):
    df = pd.DataFrame(
        array, columns=["timestamp", "order_side", "open", "high", "low", "close", "vol", "count"])
    df['timestamp'] = pd.to_datetime(
        (df["timestamp"]), utc=True, unit='us')
    df = df.set_index('timestamp')

    df['order_side'] = df['order_side'].map(decode_order_side)

    return df


class BaseAgent:
    def __init__(self):
        self.session = None
    
    def initialize(self, session):
        self.session = session
    
    def clock_interval(self):
        return 60

    def _on_tick(self, time, session, price, side, size):
        self.on_tick(time, Session(session), price, side, size)

    def _on_clock(self, time, session):
        self.on_clock(time, Session(session))

    def _on_update(self, time, session, result):
        self.on_update(time, Session(session), result)


class Session:
    def __init__(self, session):
        self.session = session

    def __getattr__(self, func):
        return getattr(self.session, func)
        
    def ohlcv(self, time_window, num_of_bars, exchange_name=None, market_name=None):
        if not exchange_name:
            exchange_name = self.session.exchange_name
            market_name = self.session.market_name
        
        market = Market.get(exchange_name, market_name)

        now = self.session.current_timestamp

        return market.ohlcvv(now - time_window * num_of_bars * 1_000_000, now, time_window)




class Market:
    MARKET = {}
    DUMMY_MODE = True

    @classmethod
    def dummy_mode(cls, dummy=True):
        cls.DUMMY_MODE = dummy

    @classmethod
    def open(cls, exchange: str, market):
        exchange = exchange.upper()
        if exchange == "FTX":
            m = FtxMarket(market, cls.DUMMY_MODE)
            cls.MARKET[exchange.upper() + "/" + market.upper()] = m
            return m

    @classmethod
    def download(cls, ndays):
        for m in cls.MARKET:
            cls.MARKET[m].download(ndays)
    
    @classmethod
    def get(cls, exchange, market):
        return cls.MARKET[exchange.upper() + "/" + market.upper()]



class FtxMarket:
    def __init__(self, name, dummy=True):
        self.dummy = dummy
        self.ftx = _FtxMarket(name, dummy)
        self.exchange_name = "FTX"
        self.market_name = name

    def select_trades(self, from_time, to_time):
        return trades_to_df(self.ftx.select_trades(from_time, to_time))

    def ohlcvv(self, from_time, to_time, window_sec):
        return ohlcvv_to_df(self.ftx.ohlcvv(from_time, to_time, window_sec))

    def download(self, ndays, force=False):
        return self.ftx.download(ndays, force)

    def __getattr__(self, func):
        return getattr(self.ftx, func)

