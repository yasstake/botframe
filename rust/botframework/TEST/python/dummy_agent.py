# -*- coding: utf8 -*-

import rbot


class Agent:
    def on_event(self, time_ms, action, price, size):
        pass
        #//print("E", time_ms, action, price, size)

    def on_tick(self, time_ms, session):
        #print("c", time_ms, market.current_time)
        print("start", rbot.PrintTime(time_ms))        
        # TODO: 出たーがなかったときの処理。
        #session.ohlcv(10, 480)
        # print(market.log_start_ms)
        #ohlcv = bc.log_ohlcv(time_ms, 60, 100)

        #print(ohlcv)
        return rbot.Order("buy", 1000, 100, 600)


    
    def on_update(self, result):
        print("update", result.status)
        



bb = rbot.DummyBb()
bb.log_load(2)
bb.debug_loop_count = 20

agent = Agent()

bb.run(agent, 60)

dir(bb)

print(bb.transactions)


