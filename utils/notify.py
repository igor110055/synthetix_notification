from discord import Webhook, RequestsWebhookAdapter, Embed
from eth_event import decode_log
import logging, watchtower
from logging.handlers import TimedRotatingFileHandler
import boto3
from utils.utility import process_data
import sys
import asyncio
import pandas as pd
import nest_asyncio
nest_asyncio.apply()

class Notify():
    
    def __init__(self,conf):
        self.conf = conf
        self.log_init()
    
    async def process_notifications(self):
        self.notifierActive = True
        try:
            while self.notifierActive == True:
                for idx, socketDict in enumerate(self.socketList):
                    if len(socketDict["output"])>0:
                        self.socketDict = socketDict
                        if "futures_position_modified" in socketDict["eventId"]:
                            eventId = 'futures_position_modified'
                        else:
                            eventId = socketDict["eventId"]
                        exec(f'''self.process_{eventId}()''')
                        self.socketList[idx]["output"] = list()
                await asyncio.sleep(15)
        #On cancellation, cancel subscription
        except KeyboardInterrupt:
            self.notifierActive = False
            
        except asyncio.CancelledError:
            self.log(message="notify disconnection on task cancellation",isWarning=False)

        #On Inordinary exception regenerate the socket
        except:
            self.logger.exception('issue with notification')
            sys.exit(1)
            
    def process_synth_minting(self):
        etherscanLink = self.conf["etherscan"]["links"][self.socketDict["network"]]        
        for log in self.socketDict["output"]:
            outputDict = self.process_log(log)
            df = pd.DataFrame.from_dict({'sent to': [outputDict["destination"][:8]],
                                         'sent from': [('0x'+log["topics"][2][-40:])[:8]],
                                         'sent on': [self.socketDict["network"]],
                                         'ccy': [outputDict["currencyKey"]],
                                         'amount': [str(outputDict["amount"])],
                                         'tx': [f'''[{log["transactionHash"][:8]}]({etherscanLink.format(log["transactionHash"])})''']}).T
            df["synth minting"] = df.index
            df.columns=[f'''{int(log["blockNumber"],16)}''',"synth minting"]
            df = df[df.columns[::-1]]
            hook = self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["discordHook"]
            self.print_embed(hook=hook, df=df)

    def process_synth_burning(self):
        etherscanLink = self.conf["etherscan"]["links"][self.socketDict["network"]]        
        for log in self.socketDict["output"]:
            outputDict = self.process_log(log)
            df = pd.DataFrame.from_dict({'sent to': [outputDict["destination"][:8]],
                                         'sent from': [('0x'+log["topics"][2][-40:])[:8]],
                                         'sent on': [self.socketDict["network"]],
                                         'ccy': [outputDict["currencyKey"]],
                                         'amount': [str(outputDict["amount"])],
                                         'tx': [f'''[{log["transactionHash"][:8]}]({etherscanLink.format(log["transactionHash"])})''']}).T
            df["synth minting"] = df.index
            df.columns=[f'''{int(log["blockNumber"],16)}''',"synth burning"]
            df = df[df.columns[::-1]]
            hook = self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["discordHook"]
            self.print_embed(hook=hook, df=df)

    def process_synth_exchange(self):
        etherscanLink = self.conf["etherscan"]["links"][self.socketDict["network"]]        
        for log in self.socketDict["output"]:
            outputDict = self.process_log(log)
            df = pd.DataFrame.from_dict({'from': [str(outputDict["fromAmount"]) + " " + outputDict["fromCurrencyKey"]],
                                         'to': [str(outputDict["toAmount"]) +" " + outputDict["toCurrencyKey"]],
                                         'user': [f'''[{outputDict["account"][:8]}]({etherscanLink.format(log["transactionHash"])})''']}).T
            df["synth trade"] = df.index
            df.columns=[f'''{int(log["blockNumber"],16)}''',"synth_trade"]
            df = df[df.columns[::-1]]
            hook = self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["discordHook"]
            if outputDict["fromCurrencyKey"] == 'sUSD':
                if outputDict["fromAmount"] > 10000:
                    hook = hook["big"]
                else:
                    hook = hook["small"]
            elif outputDict["toCurrencyKey"] == 'sUSD':
                if outputDict["toAmount"] > 10000:
                    hook = hook["big"]
                else:
                    hook = hook["small"]
            else:
                hook = hook["big"]
            self.print_embed(hook=hook, df=df)
                        
    def process_futures_position_modified(self):
        etherscanLink = self.conf["etherscan"]["links"][self.socketDict["network"]]
        ticker        = self.socketDict["eventId"].split("_")[-1]
        for log in self.socketDict["output"]:
            outputDict = self.process_log(log)
            df = pd.DataFrame.from_dict({'margin': [str(outputDict["margin"])],
                                         'size': [str(outputDict["size"])],
                                         'tradeSize': [str(outputDict["tradeSize"])],
                                         'lastPrice': [str(outputDict["lastPrice"])],
                                         'fee': [str(outputDict["fee"])],
                                         'trader': [f'''[{outputDict["account"][:8]}]({etherscanLink.format(log["transactionHash"])})''']}).T
            df["futures trade"] = df.index
            tradeDirection = 'long' if outputDict["tradeSize"]>0 else 'short'
            df.columns=[f'''{tradeDirection}  {ticker}''',"futures trade"]            
            df = df[df.columns[::-1]]
            hook = self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["discordHook"]
            if outputDict["tradeSize"]*outputDict["lastPrice"] > 10000:
                hook = hook["big"]
            else:
                hook = hook["small"]
            self.print_embed(hook=hook, df=df)
    
    def process_log(self,log):
        decodedLog = decode_log(log=log, 
                                topic_map=self.socketDict["topicMap"])        
        df = pd.DataFrame(decodedLog["data"])
        df["data"] = df[["type","value"]].apply(process_data,axis=1)
        df.index=df["name"]
        df = df["data"]
        return df.to_dict()
    
    def print_embed(self,hook,df,title=None):
        
        webhook     = Webhook.from_url(hook, adapter=RequestsWebhookAdapter())    
        columns     = df.columns
        valuesList  = df.values.tolist()
                
        e = Embed()
        for idx, columnName in enumerate(columns):
            string = ''
            for value in valuesList:
                string =  string +  "\n" + value[idx]
            e.add_field(name=columnName, 
                        value=string,
                        inline=True)
        webhook.send(embed=e)

    def log_init(self):
        logging.basicConfig(handlers=[logging.FileHandler(filename="log.log", 
                                                         encoding='utf-8', mode='a+')],
                            format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                            datefmt="%F %A %T", 
                            level=logging.INFO)                                
        self.logger = logging.getLogger(__name__)
        handler = TimedRotatingFileHandler(filename='log.log', when='D', interval=1, backupCount=5, encoding='utf-8', delay=True)
        self.logger.addHandler(handler)
        try:            
            boto3Client = boto3.client("logs",
                                       aws_access_key_id=self.conf["aws"]["keyId"],
                                       aws_secret_access_key=self.conf["aws"]["secretKey"],
                                       region_name=self.conf["aws"]['region'])
            self.logger.addHandler(watchtower.CloudWatchLogHandler(boto3_client=boto3Client, 
                                                                   log_group=self.conf["aws"]['logGroup'],
                                                                   log_stream_name=self.conf["aws"]['stream'],
                                                                   create_log_stream=True))
            self.logger.debug(msg='this error was seen')
        except:
            self.logger.exception('issue streaming logs')
            
    def log(self,message,isWarning):
        if isWarning:
            self.logger.warning(message) 
        else:
            self.logger.info(message)