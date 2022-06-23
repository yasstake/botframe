import rbot



class DummyAgent:
    def on_event(self, time_ms, action, price, size):
        pass
        #//print("E", time_ms, action, price, size)

    def on_tick(self, market, time_ms):
        print("c", dir(market), time_ms)
        print(market.log_start_ms)
        #ohlcv = bc.log_ohlcv(time_ms, 60, 100)

        #print(ohlcv)
        



bb = rbot.DummyBb()
bb.log_load(2)
print(bb.log_start_ms)
print(bb.log_end_ms)
print(bb.log_ohlcv(0, 60, 100))





agent = DummyAgent()

bc = bb

print("BBB", bb.log_start_ms)
print("CCC", bc.log_start_ms)

result = rbot.sim_run(bb, agent, 5)

print("BBB", bb.log_start_ms)
print("CCC", bc.log_start_ms)



'''
session = bb.create_session()





bb.make_order("BUY", 10000.0, 10.0, 100)

h = bb.history
print(h)

ohlcv =bb.ohlcv(60)
print(ohlcv)

bb.balance = 100
print(bb.balance)

pos = bb.position

print(pos)

result = bb.run()
print(result)

result = bb.reslut
print(result)

df = pl.DataFrame()

history=bb.history
print(history)

df2 = pl.DataFrame(history)


d = pl.Datetime()
pl.datetime(year, month, day)

pl.Time()

'''
