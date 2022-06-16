import rbot



class DummyAgent:
    def event(time_ns, type, price, volume):
        print(time_ns, price, volume, side)

    def tick(time_ns):
        print(time_ns)



bb = rbot.DummyBb()
bb.load_data(20)


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

