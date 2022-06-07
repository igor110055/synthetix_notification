import websockets
import sys
import json
import asyncio
import nest_asyncio
nest_asyncio.apply()

class Websocket():

    def __init__(self):
                
        self.healIsActive    = True
        self.socketGenerator = True

    async def wss_subscribe(self,idx,endpoint,payload):
        
        async with websockets.connect(endpoint) as ws:
            #setup subscription
            await ws.send(payload)
            await ws.recv()
            try:
                #Get & process messages
                async for message in ws:            
                    self.socketList[idx]["output"].append(json.loads(message)["params"]["result"])
                    
            #On cancellation, cancel subscription
            except KeyboardInterrupt:
                pass
                
            except asyncio.CancelledError:
                self.log(message="socket disconnection on cancellation",isWarning=False)                

            #On Inordinary exception regenerate the socket
            except:    
                self.socketList[idx]["heal"] = True
                self.logger.exception('issue with socket disconnection')

    #Regenerate new tasks automatically every n minutes
    async def socket_generator(self):
        loop = asyncio.get_running_loop()
        while self.socketGenerator:
            try:
                await asyncio.sleep(60 * 30 )
                #Takes a copy of running tasks and recreates new tasks
                socketsToKill = self.socketList.copy()
                for idx, socketDict in enumerate(self.socketList):
                    task = self.wss_subscribe(idx=idx,
                                              endpoint=socketDict["endpoint"],
                                              payload=socketDict["payload"])
                    task = asyncio.ensure_future(task,loop=loop)
                    self.socketList[idx]["socket"] = task
                socketsToKill = [socketData["socket"] for socketData in socketsToKill]
                await self.cancel_socket_tasks(socketsToKill)

            except asyncio.CancelledError:
                self.log(message="generator killed",isWarning=False)
                self.taskGeneratorIsActive = False

            except KeyboardInterrupt:
                self.log(message="generator successfully killed",isWarning=False)
                self.socketGenerator = False
            except:
                self.logger.exception('issue see with socket generator, restarting bot')
                sys.exit(2)

    #Regenerate in case of error
    async def heal_check(self):
        loop = asyncio.get_running_loop()                    
        try:            
            while self.healIsActive:
                #sleep 60 seconds and check if any socket needs to be renitialized
                await asyncio.sleep(60)
                for idx, socketDict in enumerate(self.socketList):
                    if  socketDict["heal"] == True:
                        #disable heal
                        self.socketList[idx]["heal"] = False
                        #self heal has been triggered, cancel wssTasks                
                        #and create a new one
                        taskToKill = socketDict["socket"]
                        #subscribe to new socket with the same index
                        task = self.wss_subscribe(idx=idx,
                                                  endpoint=socketDict["endpoint"],
                                                  payload=socketDict["payload"])
                        task = asyncio.ensure_future(task,loop=loop)
                        self.socketList[idx]["socket"] = task
                        await self.cancel_socket_tasks([taskToKill])
        except asyncio.CancelledError:
            print("self-heal killed successfully!")
            self.healIsActive = False    
        except KeyboardInterrupt:
            self.healIsActive= False
            print("self-heal killed successfully!")
        except:
            self.logger.exception('issue with socket heal, restarting bot')
            sys.exit(2)
        
    async def cancel_socket_tasks(self,tasks):
        loop = asyncio.get_running_loop()
        for t in tasks:
            t.cancel()
        group = asyncio.gather(*tasks,return_exceptions=True)
        loop.run_until_complete(group)