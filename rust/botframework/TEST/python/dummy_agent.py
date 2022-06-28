# -*- coding: utf8 -*-

import pandas as pd

import rbot
from rbot import array_to_df
from rbot import result_to_df

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

    def on_tick_process(self, time_ms, session, order):
        print(time_ms, order)

        return order

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
