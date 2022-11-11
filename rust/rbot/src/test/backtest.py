
import numpy as np
import pandas
import pandas as pd

import rbot
from rbot import init_log;
from rbot import Market
from rbot import Session
from rbot import BaseAgent
from rbot import BackTester
from rbot import NOW
from rbot import DAYS
from rbot import OrderSide

rbot.init_log()

class Agent(BaseAgent):   
    def clock_interval(self):
        return 60    # Sec
    
    #def on_tick(self, session, time, price, side, size):
    #    print(session.current_timestamp)
    #    session.make_order(0, OrderSide.Buy, session.current_timestamp, 10.0, 100, "")

    def on_clock(self, time, session):
        print(time)
        print(session.current_timestamp)
        

bt = BackTester("FTX", "BTC-PERP")

bt.run("FTX", "BTC-PERP", Agent())


