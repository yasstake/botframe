# -*- coding: utf8 -*-

import rbot
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
             "profit", "fee", "total_profit", "pos_change", "message"]
)
    df["timestamp"] = pd.to_datetime((df["timestamp"]), utc=True, unit='ms')
    df["create_time"] = pd.to_datetime((df["create_time"]), utc=True, unit='ms')

    return df



class Agent:
    def __init__(self):
        self.K = 1.6                            # パラメターKを設定する。
        self.detect_long = False
        self.detect_short = False
        self.doten_long = False
        self.doten_short = False

    def on_tick(self, time_ms, action, price, size):
        if self.detect_long:
            order_size = 10
            message = "OpenLong"
            self.detect_long = False
            if self.doten_long:
                order_size = 20
                message = "DotenLong"
                self.doten_long = 0

            return  rbot.Order("Buy", price, order_size, 600, message)                


        if self.detect_short:
            self.detect_short = False
            order = None

            order_size = 10
            message = "OpneShort"
            if self.doten_short:
                order_size = 20
                message = "Open short"
                self.doten_short = False
                self.doten_short = 0
            
            return rbot.Order("Sell", price, order_size, 600, message)



    def on_clock(self, time_ms, session):
        ohlcv_array = session.ohlcv(60*60*2, 6)     # 最新足０番目　＋　５本の足を取得。 最新は６番目。
        ohlcv_df = array_to_df(ohlcv_array)         # ndarrayをDataFrameへ変換

        if len(ohlcv_df.index) < 6:                 # データが過去６本分そろっていない場合はリターン
            return

        # print(rbot.PrintTime(time_ms) + " on_clock")

        ohlcv_df["range"] = ohlcv_df["high"] - ohlcv_df["low"]      # レンジを計算

        ohlcv_latest = ohlcv_df[-2:-1]     # 最新足１本
        ohlcv_last_5 = ohlcv_df[:-2]       # 過去５本足

        range_width = ohlcv_last_5["range"].mean()  # 　過去５本足のレンジの平均値

        # Long/Short判定
        self.detect_long = range_width * \
            self.K < ohlcv_latest["open"][0] - ohlcv_latest["low"][0]
        if self.detect_long and session.short_pos_size:
            self.doten_long = True

        self.detect_short = range_width * \
            self.K < ohlcv_latest["high"][0] - ohlcv_latest["open"][0]
        if self.detect_short and session.long_pos_size:
            self.doten_short = True


    def on_update(self, result):
        print("event", result.status)


bb = rbot.DummyBb()
bb.log_load(20)

agent = Agent()
result = bb.run(agent, 60*60*2)

df = result_to_df(result)



print(df)

print("total  ", df["total_profit"].sum())
print("profit ", df["profit"].sum())
print("fee    ", df["fee"].sum())
