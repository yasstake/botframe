import rbot



class DummyAgent:

    def on_event(self, time_ms, action, price, size):
        print("E", time_ms, action, price, size)

    def on_tick(self, time_ms):
        print("c", time_ms)
        return str(time_ms) + "is processed"


bb = rbot.DummyBb()
bb.log_load(2)

agent = DummyAgent()

bb.run(agent, 5)



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
