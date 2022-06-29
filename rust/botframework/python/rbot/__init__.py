
from .rbot import *

import pandas as pd

def array_to_df(array):
    ohlcv_df = pd.DataFrame(
        array, columns=["timestamp", "open", "high", "low", "close", "volume"])
    ohlcv_df['timestamp'] = pd.to_datetime(
        (ohlcv_df["timestamp"]), utc=True, unit='ms')
    ohlcv_df = ohlcv_df.set_index('timestamp')

    return ohlcv_df


def result_to_df(result_list):
    timestamp = []
    order_id = []
    order_sub_id = []
    order_type = []
    post_only = []
    create_time = []
    status = []
    open_price = []
    close_price = []
    size = []
    volume = []
    profit = []
    fee = []
    total_profit = []
    position_change = []
    message = []

    for item in result_list:
        timestamp.append(item.timestamp)
        order_id.append(item.order_id)
        order_sub_id.append(item.order_sub_id)
        order_type.append(item.order_type)
        post_only.append(item.post_only)
        create_time.append(item.create_time)
        status.append(item.status)
        open_price.append(item.open_price)
        close_price.append(item.close_price)
        size.append(item.size)
        volume.append(item.volume)
        profit.append(item.profit)
        fee.append(item.fee)
        total_profit.append(item.total_profit)
        position_change.append(item.position_change)
        message.append(item.message)

    df = pd.DataFrame(
    data={"timestamp": timestamp, "order_id": order_id, "sub_id": order_sub_id,
          "order_type": order_type, "post_only": post_only, "create_time": create_time,
          "status":  status, "open_price": open_price, "close_price": close_price,
          "size": size, "volume": volume, "profit": profit, "fee": fee,
          "total_profit": total_profit, "pos_change": position_change, "message": message},
    columns=["timestamp", "order_id", "sub_id", "order_type", "post_only",
             "create_time", "status", "open_price", "close_price", "size", "volume",
             "profit", "fee", "total_profit", "pos_change", "message"])
    df["timestamp"] = pd.to_datetime((df["timestamp"]), utc=True, unit="ms")
    df["create_time"] = pd.to_datetime((df["create_time"]), utc=True, unit="ms")
    df["sum_profit"] = df["total_profit"].cumsum()
    df["sum_pos"] = df["pos_change"].cumsum()
    df = df.set_index("create_time", drop=True)

    return df




