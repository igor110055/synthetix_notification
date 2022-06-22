from utils.utility import get_w3
import pandas as pd
import sys
import asyncio
import nest_asyncio
nest_asyncio.apply()

class Recurrent():
    def __init__(self,conf):        
        self.get_synth_prices()
    
    def get_synth_prices(self):
        synthPriceDict = dict()
        for network in self.conf["nodes"].keys():            
            w3                      = get_w3(self.conf,network)
            utilsContract           = self.get_snx_contract(contractName='SynthUtil',w3=w3,network=network)
            synthSupplyList         = utilsContract.functions.synthsTotalSupplies().call()
            synthDF                 = pd.DataFrame(synthSupplyList).T
            synthDF.columns         = ['currencyKeyHex','supply','cap']        
            synthDF["currencyKey"]  = synthDF["currencyKeyHex"].str.decode('utf-8').str.replace('\x00','')
            synthDF                 = synthDF.query("supply > 0").copy()
            synthDF["price"]        = synthDF["cap"] / synthDF["supply"]
            synthDF.index           = synthDF["currencyKey"]
            synthDF                 = synthDF["price"]
            synthPriceDict = {**synthPriceDict, **synthDF.to_dict()}        
        self.synthPriceDict = synthPriceDict
    
    async def get_synth_prices_recurrent(self):
        try:
            while True:
                await asyncio.sleep(60*60)
                self.get_synth_prices()
        except KeyboardInterrupt:
            sys.exit(0)
        except asyncio.CancelledError:
            self.log(message="snx contract update killed",isWarning=False)
            sys.exit(0)
        except:
            self.logger.exception('issue with snx synth price fetch')
            sys.exit(1)

    async def restart_recurrent(self):
        await asyncio.sleep(60*60*3)
        sys.exit(6)