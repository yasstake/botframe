import pandas as pd

import rbot
from rbot import array_to_df
from rbot import result_to_df

class Agent:
    def __init__(self, param_K=1.6):
            self.K = param_K                           # パラメターKを設定する。

    def on_clock(self, time_ms: int, session):
        ohlcv_array = session.ohlcv(60*60*2, 6)     # 最新足０番目　＋　５本の足を取得。 最新は６番目。
        ohlcv_df = rbot.array_to_df(ohlcv_array)         # ndarrayをDataFrameへ変換

        if len(ohlcv_df.index) < 6:                 # データが過去６本分そろっていない場合はなにもせずリターン
            return 

        print(rbot.PrintTime(time_ms) + " ontick")

        ohlcv_df["range"] = ohlcv_df["high"] - ohlcv_df["low"]      # レンジを計算

        ohlcv_latest = ohlcv_df[-2:-1]     # 最新足１本
        ohlcv_last_5 = ohlcv_df[:-2]       # 過去５本足

        range_width = ohlcv_last_5["range"].mean()      #　過去５本足のレンジの平均値

        # Long/Short判定
        detect_short = range_width * self.K < ohlcv_latest["high"][0] - ohlcv_latest["open"][0]
        detect_long  = range_width * self.K < ohlcv_latest["open"][0] - ohlcv_latest["low"][0]

        #　執行方法（まずはシンプルとにかくオーダを出す）
        if detect_long:
            print("make long")
            return rbot.Order("Buy", session.buy_edge_price, 10, 600, "Open Long")    

        if detect_short:
            print("make short")            
            return rbot.Order("Sell", session.sell_edge_price, 10, 600, "Open Short")  


bb = rbot.DummyBb()
bb.log_load(20)

agent = Agent()
result = bb.run(agent, 60*60*2)

df = result_to_df(result)



print(df)

print("total  ", df["total_profit"].sum())
print("profit ", df["profit"].sum())
print("fee    ", df["fee"].sum())
