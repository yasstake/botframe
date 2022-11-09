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


class FtxMarket:
    def __init__(self, name, dummy=True):
        self.dummy = dummy
        self.ftx = rbot._FtxMarket(name, dummy)

    def select_trades(self, from_time, to_time):
        return trades_to_df(self.ftx.select_trades(from_time, to_time))

    def ohlcvv(self, from_time, to_time, window_sec):
        return ohlcvv_to_df(self.ftx.ohlcvv(from_time, to_time, window_sec))

    def download(self, ndays, force=False):
        return self.ftx.download(ndays, force)

    def __getattr__(self, func):
        return getattr(self.ftx, func)


class Session:
    def __init__(self):
        self.session = rbot._DummySession()

