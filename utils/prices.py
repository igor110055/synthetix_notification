# -*- coding: utf-8 -*-
import ccxt
import sys
from utils.utility import get_w3

class Prices:
    def __init__(self,conf):
        self.exchange = ccxt.binance()
        self.markets  = self.exchange.load_markets()        
        w3 = get_w3(conf,'ethereum')
        self.exchangerRatesContract = self.get_snx_contract('ExchangeRates',w3,'ethereum')

    def get_binance_price(self,ticker):
        
        if ticker.lower() == 'usd':
            return 1
        
        symbol = f'{ticker.upper()}/USDT'
        
        if not symbol in self.markets:
            return -1
        try:
            return self.exchange.fetch_ticker(symbol)["close"]
        except ccxt.RequestTimeout:
            return -1
        except ccxt.DDoSProtection:
            return -1
        except ccxt.ExchangeNotAvailable:
            return -1
        except ccxt.ExchangeError:
            self.logger.exception('issue seen with controller restarting bot')
            sys.exit(7)
        except:
            self.logger.exception('issue seen with controller restarting bot')
            sys.exit(7)

    def get_atomic_link_price(self,synth,blockNumber):
        if synth == 'sUSD':
            return 1, 1
        atomicRate, systemRate,sourceRate,destinationRate = self.exchangerRatesContract.functions.effectiveAtomicValueAndRates('0x'+synth.encode('utf-8').hex(),1e18,'0x73555344').call(block_identifer=blockNumber)
        return atomicRate/1e18 , systemRate/1e18