

import rbot

import rbot._DummySession

from rbot import Session
#import rbot.DummySession
#from rbot import _DummySession
#from rbot import OrderResult



rbot.init_log()

ftx = rbot.FtxMarket("BTC-PERP")

# ftx.load_log(10)

# session = rbot._DummySession()

MARKET = []

def create_market(exchange, market):
    if exchange == "FTX":
        MARKET[exchange] = rbot.FtxMarket(market)

def get_market(market):
    return MARKET[market]


class BaseAgent:
    def on_tick(timestamp: int, price: float, size: float, order_side: str):
        pass
    
    def on_clock(timestamp: int, Session: Session):
        pass
    
    def on_update(timestamp: int, Session: Session, result: OrderResult):
        pass
    

class Session:
    def __init__(self, dummy: bool):
        if dummy:
            self.session = rbot._DummySession()


class MyAgent:
    def on_tick(timestamp: int, price: float, size: float, order_side: str):
        pass
    
    def on_clock(timestamp: int, Session: Session):
        pass
    
    def on_update(timestamp: int, Session: Session, result: OrderResult):
        pass
    



if __name__ == "__main__":
    print("main")
