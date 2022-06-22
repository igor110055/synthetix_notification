from utils.websocket import Websocket
from utils.snx_contracts import SnxContracts
from utils.recurrent_tasks import Recurrent
from utils.notify import Notify
from utils.loans import Loans
from utils.prices import Prices
from web3.providers.base import JSONBaseProvider
from utils.utility import get_abi
from eth_event import get_topic_map
from web3 import Web3
import json
import sys
import asyncio
import nest_asyncio
nest_asyncio.apply()

class Controller(Websocket,SnxContracts,Notify,Recurrent,Loans,Prices):

    def __init__(self,conf,socketConf):
        
        self.conf       = conf
        self.socketConf = socketConf       
        Websocket.__init__(self)
        SnxContracts.__init__(self,conf=conf)
        Notify.__init__(self,conf)
        Recurrent.__init__(self,conf)
        Loans.__init__(self,conf)
        Prices.__init__(self,conf)
        
    def trigger_controller(self):
                
        self.log("starting notification bot",isWarning=False)

        #Create Task Creator and Destructor
        loop = asyncio.get_event_loop()
        
        master = list()

        #websockets
        
        #initial socket tasks & add self-heal tasks
        self.initialize_socket_list()
        master.extend(socketDict["socket"] for socketDict in self.socketList)
        master.append(self.heal_check())
        master.append(self.socket_generator())
        
        #add recurrent tasks / fetching snx contracts / fetching synth prices / restarting the bot
        master.append(self.get_synth_prices_recurrent())
        master.append(self.restart_recurrent())

        #add notification bot
        master.append(self.process_notifications())
                
        #Group Them in 1 Loop
        master = asyncio.gather(*master,return_exceptions=True)

        try:
            loop.run_until_complete(master)
        except KeyboardInterrupt:
            tasks = asyncio.all_tasks(loop=loop)
            for t in tasks:
                t.cancel()
            group = asyncio.gather(*tasks,return_exceptions=True)
            loop.run_until_complete(group)
            print("controller killed successfully!")
        except:
            self.logger.exception('issue seen with controller restarting bot')
            sys.exit(3)
    
    def initialize_socket_list(self):
        loop = asyncio.get_event_loop()
        self.socketList = list()
        idx  = 0
        for network in self.socketConf.keys():
            for eventId in self.socketConf[network]:                
                socketDict = self.socketConf[network][eventId]                
                address    = self.get_address(contractName=socketDict["contractName"], network=network)
                abiAddress = self.get_address(contractName=socketDict["abi"], network=network)
                abi        = get_abi(conf=self.conf, address=abiAddress, network=network)  
                topicMap   = get_topic_map(json.loads(abi))
                
                payload    = self.prepare_payload(address=address,
                                                topic=socketDict["topic"])
                endpoint   = self.conf["nodes"][network]["websocket"]
                task       = self.wss_subscribe(idx=idx,endpoint=endpoint,payload=payload)
                task       = asyncio.ensure_future(task,loop=loop)
                data       = {'payload': payload,                            
                              'heal': False,
                              'endpoint': endpoint,
                              'socket': task,
                              'output': list(),
                              'network':network,
                              'eventId':eventId,
                              'topicMap':topicMap}
                self.socketList.append(data)
                idx+=1
        
    def prepare_payload(self,address,topic):
        baseProvider  = JSONBaseProvider()
        params        = ["logs", {"topics": [Web3.keccak(text=topic).hex()],
                                  'address':address}]
        return baseProvider.encode_rpc_request(method='eth_subscribe', params=params) 
#%%
if __name__ =='__main__':
    self=Controller(conf,socketConf)
    self.trigger_controller()