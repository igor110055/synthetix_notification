import sys
import traceback
from funcy import chunks
from web3.providers.base import JSONBaseProvider
from utils.utility import get_abi, get_w3, run_post_request
from itertools import cycle
from web3._utils.abi import get_abi_output_types
import aiohttp
import asyncio
import nest_asyncio

class MultiCall():

    def __init__(self,conf):
        self.conf     = conf
        self.decoder  = list()
        self.load_multicall()
        
    def load_multicall(self):
        self.multicallContract = dict()
        for network, address in self.conf["multicaller"].items():
            w3  = get_w3(conf=self.conf,network=network)
            abi = get_abi(conf=self.conf,address=address,network=network)
            self.multicallContract[network] = w3.eth.contract(address=address,abi=abi)        
        decoder = self.multicallContract[network].get_function_by_name(fn_name='aggregate')
        self.multicallDecoder = get_abi_output_types(decoder.abi)
                
    #can do 1 contract at a time, with 1 function hitting it with a list of args
    def trigger_multicall(self, contract, functionName, argList,network,blockNumber='latest'):

        payloadList  = list()
        w3  = get_w3(conf=self.conf,network=network)
        fn = contract.get_function_by_name(fn_name=functionName)
        decoder = get_abi_output_types(fn.abi)

        #loop on args 1000 at a time (i.e. 1k addresses for example)
        for argChunk in chunks(1000,argList):
            temp = list()
            #loop on individual args
            for arg in argChunk:
                #encode the function
                callData = contract.encodeABI(fn_name=functionName, args=[arg])
                callData = (contract.address, callData)
                temp.append(callData)
            #save the individalu multicall calls into self.payload
            payloadList.append(self.multicallContract[network].encodeABI(fn_name='aggregate',args=[temp]))
        
        task = [self.run_multicall(blockNumber=blockNumber,network=network,payloadList=payloadList)]

        loop  = asyncio.get_event_loop()        
        tasks = asyncio.gather(*task,return_exceptions=True)
        responsesList = loop.run_until_complete(tasks)
        responsesList = responsesList[0]
        
        outputList = list()
        for response in responsesList:
            outputs = w3.codec.decode_abi(self.multicallDecoder, w3.toBytes(hexstr=response["result"]))[1]
            for output in outputs:
                outputList.append(w3.codec.decode_abi(decoder,output)[0])
        
        return {arg: output for arg, output in zip(argList,outputList)}
                    
    async def run_multicall(self,blockNumber,network,payloadList):

        if blockNumber == 'latest':
            rpcCycle    = cycle(self.conf['rpc'][network])
            nodeCount   = len(self.conf["rpc"][network])
        else:
            rpcCycle = cycle(self.conf["rpc"][network])
            nodeCount   = len(self.conf["rpc"][network])

        responsesList = list()

        for payloadChunk in chunks(nodeCount,payloadList):
            baseProvider  = JSONBaseProvider()
            tasks         = list()
            counter = 0
            while True:                
                try:
                    if counter > 6:
                        sys.exit("unable to run multicall despite several attempts")
                    async with aiohttp.ClientSession() as session:
                        for payload in payloadChunk:
                            params=[{'from':'0x0000000000000000000000000000000000000000',
                                     'to':self.multicallContract[network].address,
                                     'data':payload},
                                    hex(blockNumber)]
                            data = baseProvider.encode_rpc_request('eth_call',params)
                            task = run_post_request(session=session,
                                                    url=next(rpcCycle),
                                                    data=data)
                            tasks.append(task)
                        responsesList.extend(await asyncio.gather(*tasks))
                        break
                except KeyboardInterrupt:
                    break

                except Exception:
                    counter+=1
                    print("error seen while running multicall trying again")
                    print(traceback.format_exc())
                    await asyncio.sleep(2)
           
        return responsesList