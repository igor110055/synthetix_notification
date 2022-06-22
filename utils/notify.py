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
            sys.exit(0)
            
        except asyncio.CancelledError:
            self.log(message="notify disconnection on task cancellation",isWarning=False)
            sys.exit(0)

        #On Inordinary exception regenerate the socket
        except:
            self.log(f"issue seen with {self.socketDict}",isWarning=True)
            self.logger.exception('issue with notification')
            sys.exit(1)
            
    def process_shorts_open(self):
        etherscanLink = self .conf["etherscan"]["links"][self.socketDict["network"]]        
        for log in self.socketDict["output"]:
            outputDict, undecodedDict = self.process_log(log)
            usdValue = self.synthPriceDict.get(outputDict["currency"],0)*outputDict["amount"]
            df = pd.DataFrame.from_dict({'account': [outputDict["account"][:8]],
                                         'sent on': [self.socketDict["network"]],
                                         'ccy': [outputDict["currency"]],
                                         'amount': ["{0:,.2f}".format(outputDict["amount"])],
                                         'usdValue':["{0:,.2f}".format(usdValue)],
                                         'tx': [f'''[{log["transactionHash"][:8]}]({etherscanLink.format(log["transactionHash"])})''']}).T
            df[self.socketDict["eventId"]] = df.index
            df.columns=[f'''{int(log["blockNumber"],16)}''',self.socketDict["eventId"]]
            df = df[df.columns[::-1]]
            hook = self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["discordHook"]
            if usdValue > 10000:
                hook = hook["big"]    
            else:
                hook = hook["small"]
            self.print_embed(hook=hook, df=df)

    def process_shorts_increase(self):
        etherscanLink = self.conf["etherscan"]["links"][self.socketDict["network"]]
        for log in self.socketDict["output"]:
            outputDict, undecodedDict = self.process_log(log)            
            userAddress = '0x'+log["topics"][1][-40:]
            currencyKey = self.get_loan_ccy(loanId=undecodedDict["id"],
                                            userAddress=userAddress,
                                            network=self.socketDict["network"],
                                            contractName=self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["contractName"])
            usdValue = self.synthPriceDict.get(currencyKey,0)*outputDict["amount"]
            df = pd.DataFrame.from_dict({'account': [outputDict["account"][:8]],
                                         'sent on': [self.socketDict["network"]],
                                         'ccy': [currencyKey],
                                         'amount': ["{0:,.2f}".format(outputDict["amount"])],
                                         'usdValue':["{0:,.2f}".format(usdValue)],
                                         'tx': [f'''[{log["transactionHash"][:8]}]({etherscanLink.format(log["transactionHash"])})''']}).T
            df[self.socketDict["eventId"]] = df.index
            df.columns=[f'''{int(log["blockNumber"],16)}''',self.socketDict["eventId"]]
            df = df[df.columns[::-1]]
            hook = self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["discordHook"]
            if usdValue > 10000:
                hook = hook["big"]    
            else:
                hook = hook["small"]
            self.print_embed(hook=hook, df=df)

    def process_shorts_decrease(self):
        etherscanLink = self.conf["etherscan"]["links"][self.socketDict["network"]]
        for log in self.socketDict["output"]:
            outputDict, undecodedDict = self.process_log(log)            
            userAddress = '0x'+log["topics"][1][-40:]
            currencyKey = self.get_loan_ccy(loanId=undecodedDict["id"],
                                            userAddress=userAddress,
                                            network=self.socketDict["network"],
                                            contractName=self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["contractName"])
            usdValue = self.synthPriceDict.get(currencyKey,0)*outputDict["amountRepaid"]
            df = pd.DataFrame.from_dict({'account': [outputDict["account"][:8]],
                                         'sent on': [self.socketDict["network"]],
                                         'ccy': [currencyKey],
                                         'amount': ["{0:,.2f}".format(outputDict["amountRepaid"])],
                                         'usdValue':["{0:,.2f}".format(usdValue)],
                                         'tx': [f'''[{log["transactionHash"][:8]}]({etherscanLink.format(log["transactionHash"])})''']}).T
            df[self.socketDict["eventId"]] = df.index
            df.columns=[f'''{int(log["blockNumber"],16)}''',self.socketDict["eventId"]]
            df = df[df.columns[::-1]]
            hook = self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["discordHook"]
            if usdValue > 10000:
                hook = hook["big"]    
            else:
                hook = hook["small"]
            self.print_embed(hook=hook, df=df)

    def process_eth_loan_open(self):
        etherscanLink = self .conf["etherscan"]["links"][self.socketDict["network"]]        
        for log in self.socketDict["output"]:
            outputDict, undecodedDict = self.process_log(log)
            usdValue = self.synthPriceDict.get(outputDict["currency"],0)*outputDict["amount"]
            df = pd.DataFrame.from_dict({'account': [outputDict["account"][:8]],
                                         'sent on': [self.socketDict["network"]],
                                         'ccy': [outputDict["currency"]],
                                         'amount': ["{0:,.2f}".format(outputDict["amount"])],
                                         'usdValue':["{0:,.2f}".format(usdValue)],
                                         'tx': [f'''[{log["transactionHash"][:8]}]({etherscanLink.format(log["transactionHash"])})''']}).T
            df[self.socketDict["eventId"]] = df.index
            df.columns=[f'''{int(log["blockNumber"],16)}''',self.socketDict["eventId"]]
            df = df[df.columns[::-1]]
            hook = self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["discordHook"]
            if usdValue > 10000:
                hook = hook["big"]    
            else:
                hook = hook["small"]
            self.print_embed(hook=hook, df=df)

    def process_eth_loan_increase(self):
        etherscanLink = self.conf["etherscan"]["links"][self.socketDict["network"]]
        for log in self.socketDict["output"]:
            outputDict, undecodedDict = self.process_log(log)            
            userAddress = '0x'+log["topics"][1][-40:]
            currencyKey = self.get_loan_ccy(loanId=undecodedDict["id"],
                                            userAddress=userAddress,
                                            network=self.socketDict["network"],
                                            contractName=self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["contractName"])
            usdValue = self.synthPriceDict.get(currencyKey,0)*outputDict["amount"]
            df = pd.DataFrame.from_dict({'account': [outputDict["account"][:8]],
                                         'sent on': [self.socketDict["network"]],
                                         'ccy': [currencyKey],
                                         'amount': ["{0:,.2f}".format(outputDict["amount"])],
                                         'usdValue':["{0:,.2f}".format(usdValue)],
                                         'tx': [f'''[{log["transactionHash"][:8]}]({etherscanLink.format(log["transactionHash"])})''']}).T
            df[self.socketDict["eventId"]] = df.index
            df.columns=[f'''{int(log["blockNumber"],16)}''',self.socketDict["eventId"]]
            df = df[df.columns[::-1]]
            hook = self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["discordHook"]
            if usdValue > 10000:
                hook = hook["big"]    
            else:
                hook = hook["small"]
            self.print_embed(hook=hook, df=df)

    def process_eth_loan_decrease(self):
        etherscanLink = self.conf["etherscan"]["links"][self.socketDict["network"]]
        for log in self.socketDict["output"]:
            outputDict, undecodedDict = self.process_log(log)            
            userAddress = '0x'+log["topics"][1][-40:]
            currencyKey = self.get_loan_ccy(loanId=undecodedDict["id"],
                                            userAddress=userAddress,
                                            network=self.socketDict["network"],
                                            contractName=self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["contractName"])
            usdValue = self.synthPriceDict.get(currencyKey,0)*outputDict["amountRepaid"]
            df = pd.DataFrame.from_dict({'account': [outputDict["account"][:8]],
                                         'sent on': [self.socketDict["network"]],
                                         'ccy': [currencyKey],
                                         'amount': ["{0:,.2f}".format(outputDict["amountRepaid"])],
                                         'usdValue':["{0:,.2f}".format(usdValue)],
                                         'tx': [f'''[{log["transactionHash"][:8]}]({etherscanLink.format(log["transactionHash"])})''']}).T
            df[self.socketDict["eventId"]] = df.index
            df.columns=[f'''{int(log["blockNumber"],16)}''',self.socketDict["eventId"]]
            df = df[df.columns[::-1]]
            hook = self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["discordHook"]
            if usdValue > 10000:
                hook = hook["big"]    
            else:
                hook = hook["small"]
            self.print_embed(hook=hook, df=df)

    def process_erc_loan_open(self):
        etherscanLink = self .conf["etherscan"]["links"][self.socketDict["network"]]        
        for log in self.socketDict["output"]:
            outputDict, undecodedDict = self.process_log(log)
            usdValue = self.synthPriceDict.get(outputDict["currency"],0)*outputDict["amount"]
            df = pd.DataFrame.from_dict({'account': [outputDict["account"][:8]],
                                         'sent on': [self.socketDict["network"]],
                                         'ccy': [outputDict["currency"]],
                                         'amount': ["{0:,.2f}".format(outputDict["amount"])],
                                         'usdValue':["{0:,.2f}".format(usdValue)],
                                         'tx': [f'''[{log["transactionHash"][:8]}]({etherscanLink.format(log["transactionHash"])})''']}).T
            df[self.socketDict["eventId"]] = df.index
            df.columns=[f'''{int(log["blockNumber"],16)}''',self.socketDict["eventId"]]
            df = df[df.columns[::-1]]
            hook = self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["discordHook"]
            if usdValue > 10000:
                hook = hook["big"]    
            else:
                hook = hook["small"]
            self.print_embed(hook=hook, df=df)

    def process_erc_loan_increase(self):
        etherscanLink = self.conf["etherscan"]["links"][self.socketDict["network"]]
        for log in self.socketDict["output"]:
            outputDict, undecodedDict = self.process_log(log)            
            userAddress = '0x'+log["topics"][1][-40:]
            currencyKey = self.get_loan_ccy(loanId=undecodedDict["id"],
                                            userAddress=userAddress,
                                            network=self.socketDict["network"],
                                            contractName=self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["contractName"])
            usdValue = self.synthPriceDict.get(currencyKey,0)*outputDict["amount"]
            df = pd.DataFrame.from_dict({'account': [outputDict["account"][:8]],
                                         'sent on': [self.socketDict["network"]],
                                         'ccy': [currencyKey],
                                         'amount': ["{0:,.2f}".format(outputDict["amount"])],
                                         'usdValue':["{0:,.2f}".format(usdValue)],
                                         'tx': [f'''[{log["transactionHash"][:8]}]({etherscanLink.format(log["transactionHash"])})''']}).T
            df[self.socketDict["eventId"]] = df.index
            df.columns=[f'''{int(log["blockNumber"],16)}''',self.socketDict["eventId"]]
            df = df[df.columns[::-1]]
            hook = self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["discordHook"]
            if usdValue > 10000:
                hook = hook["big"]    
            else:
                hook = hook["small"]
            self.print_embed(hook=hook, df=df)

    def process_erc_loan_decrease(self):
        etherscanLink = self.conf["etherscan"]["links"][self.socketDict["network"]]
        for log in self.socketDict["output"]:
            outputDict, undecodedDict = self.process_log(log)            
            userAddress = '0x'+log["topics"][1][-40:]
            currencyKey = self.get_loan_ccy(loanId=undecodedDict["id"],
                                            userAddress=userAddress,
                                            network=self.socketDict["network"],
                                            contractName=self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["contractName"])
            usdValue = self.synthPriceDict.get(currencyKey,0)*outputDict["amountRepaid"]
            df = pd.DataFrame.from_dict({'account': [outputDict["account"][:8]],
                                         'sent on': [self.socketDict["network"]],
                                         'ccy': [currencyKey],
                                         'amount': ["{0:,.2f}".format(outputDict["amountRepaid"])],
                                         'usdValue':["{0:,.2f}".format(usdValue)],
                                         'tx': [f'''[{log["transactionHash"][:8]}]({etherscanLink.format(log["transactionHash"])})''']}).T
            df[self.socketDict["eventId"]] = df.index
            df.columns=[f'''{int(log["blockNumber"],16)}''',self.socketDict["eventId"]]
            df = df[df.columns[::-1]]
            hook = self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["discordHook"]
            if usdValue > 10000:
                hook = hook["big"]    
            else:
                hook = hook["small"]
            self.print_embed(hook=hook, df=df)

    def process_synth_minting(self):
        etherscanLink = self.conf["etherscan"]["links"][self.socketDict["network"]]        
        for log in self.socketDict["output"]:
            outputDict, undecodedDict = self.process_log(log)
            df = pd.DataFrame.from_dict({'sent to': [outputDict["destination"][:8]],
                                         'sent on': [self.socketDict["network"]],
                                         'ccy': [outputDict["currencyKey"]],
                                         'amount': ["{0:,.2f}".format(outputDict["amount"])],
                                         'tx': [f'''[{log["transactionHash"][:8]}]({etherscanLink.format(log["transactionHash"])})''']}).T
            df[self.socketDict["eventId"]] = df.index
            df.columns=[f'''{int(log["blockNumber"],16)}''',self.socketDict["eventId"]]
            df = df[df.columns[::-1]]
            hook = self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["discordHook"]
            self.print_embed(hook=hook, df=df)

    def process_synth_burning(self):
        etherscanLink = self.conf["etherscan"]["links"][self.socketDict["network"]]        
        for log in self.socketDict["output"]:
            outputDict, undecodedDict = self.process_log(log)
            df = pd.DataFrame.from_dict({'sent to': [outputDict["destination"][:8]],
                                         'sent on': [self.socketDict["network"]],
                                         'ccy': [outputDict["currencyKey"]],
                                         'amount': ["{0:,.2f}".format(outputDict["amount"])],
                                         'tx': [f'''[{log["transactionHash"][:8]}]({etherscanLink.format(log["transactionHash"])})''']}).T
            df[self.socketDict["eventId"]] = df.index
            df.columns=[f'''{int(log["blockNumber"],16)}''',self.socketDict["eventId"]]
            df = df[df.columns[::-1]]
            hook = self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["discordHook"]
            self.print_embed(hook=hook, df=df)

    def process_synth_exchange(self):
        etherscanLink = self.conf["etherscan"]["links"][self.socketDict["network"]]        
        for log in self.socketDict["output"]:
            outputDict, undecodedDict = self.process_log(log)
            if outputDict["fromCurrencyKey"] == 'sUSD':
                usdValue = outputDict["fromAmount"]
            elif outputDict["toCurrencyKey"] == 'sUSD':
                usdValue = outputDict["toAmount"]
            else:                    
                usdValue = self.synthPriceDict.get(outputDict["fromCurrencyKey"],0)*outputDict["fromAmount"]
            df = pd.DataFrame.from_dict({'from': [str("{0:,.2f}".format(outputDict["fromAmount"])) + " " + outputDict["fromCurrencyKey"]],
                                         'to': [str("{0:,.2f}".format(outputDict["toAmount"])) +" " + outputDict["toCurrencyKey"]],
                                         'user': [f'''[{outputDict["account"][:8]}]({etherscanLink.format(log["transactionHash"])})'''],
                                         'trade value': "{0:,.2f} sUSD ".format(usdValue)}).T
            df[self.socketDict["eventId"]] = df.index
            df.columns=[f'''{int(log["blockNumber"],16)}''',self.socketDict["eventId"]]
            df = df[df.columns[::-1]]
            hook = self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["discordHook"]
            if usdValue>10000:
                hook = hook["big"]
            else:
                hook = hook["small"]
            self.print_embed(hook=hook, df=df)

    def process_atomic_exchange(self):
        etherscanLink = self.conf["etherscan"]["links"][self.socketDict["network"]]        
        for log in self.socketDict["output"]:
            outputDict, undecodedDict = self.process_log(log)            
            binanceSourceRate      = self.get_binance_price(outputDict["fromCurrencyKey"][1:])
            binanceDestinationRate = self.get_binance_price(outputDict["toCurrencyKey"][1:])
            atomicSourceRate, chainlinkSourceRate           = self.get_atomic_link_price(outputDict["fromCurrencyKey"],blockNumber=log["blockNumber"])
            atomicDestinationRate, chainlinkDestinationRate = self.get_atomic_link_price(outputDict["toCurrencyKey"],blockNumber=int(log["blockNumber"],16))
            
            df = pd.DataFrame.from_dict({'from': [str("{0:,.2f}".format(outputDict["fromAmount"])) + " " + outputDict["fromCurrencyKey"]],
                                         'to': [str("{0:,.2f}".format(outputDict["toAmount"])) +" " + outputDict["toCurrencyKey"]],
                                         'user': [f'''[{outputDict["account"][:8]}]({etherscanLink.format(log["transactionHash"])})'''],
                                         'atomicSourceRate':"{0:,.4f}".format(atomicSourceRate),
                                         'binanceSourceRate':"{0:,.4f}".format(binanceSourceRate),
                                         'sourceDelta': int(abs(atomicSourceRate/binanceSourceRate-1)*1e4),
                                         'atomicDestinationRate':"{0:,.4f}".format(atomicDestinationRate),
                                         'binanceDestinationRate':"{0:,.4f}".format(binanceDestinationRate),
                                         'destinationDelta': int(abs(atomicDestinationRate/binanceDestinationRate-1)*1e4),
                                         'chainlinkSourceRate':"{0:,.4f}".format(chainlinkSourceRate),
                                         'chainlinkDestinationRate':"{0:,.4f}".format(chainlinkDestinationRate)}).T
            df[self.socketDict["eventId"]] = df.index
            df.columns=[f'''{int(log["blockNumber"],16)}''',self.socketDict["eventId"]]
            df = df[df.columns[::-1]]
            hook = self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["discordHook"]
            self.print_embed(hook=hook, df=df)
                        
    def process_futures_position_modified(self):
        etherscanLink = self.conf["etherscan"]["links"][self.socketDict["network"]]
        ticker        = self.socketDict["eventId"].split("_")[-1]
        for log in self.socketDict["output"]:
            outputDict, undecodedDict = self.process_log(log)
            if outputDict["tradeSize"] != 0:
                df = pd.DataFrame.from_dict({'margin': [str("{0:,.2f}".format(outputDict["margin"]))],
                                             'size': [str("{0:,.2f}".format(outputDict["size"]))],
                                             'tradeSize': [str("{0:,.2f}".format(outputDict["tradeSize"]))],
                                             'tradeSizeUSD': [str("{0:,.2f}".format(outputDict["tradeSize"]*outputDict["lastPrice"]))],                       
                                             'lastPrice': [str("{0:,.2f}".format(outputDict["lastPrice"]))],
                                             'fee': [str("{0:,.2f}".format(outputDict["fee"]))],
                                             'trader': [f'''[{outputDict["account"][:8]}]({etherscanLink.format(log["transactionHash"])})''']}).T
                df["futures trade"] = df.index
                tradeDirection = 'long' if outputDict["tradeSize"]>0 else 'short'
                df.columns=[f'''{tradeDirection}  {ticker}''',"futures trade"]            
                df = df[df.columns[::-1]]
                hook = self.socketConf[self.socketDict["network"]][self.socketDict["eventId"]]["discordHook"]
                if abs(outputDict["tradeSize"]*outputDict["lastPrice"]) > 10000:
                    hook = hook["big"]
                else:
                    hook = hook["small"]
                self.print_embed(hook=hook, df=df)
            
    def process_log(self,log):
        decodedLog = decode_log(log=log, topic_map=self.socketDict["topicMap"]) 
        df = pd.DataFrame(decodedLog["data"])
        df["data"] = df[["type","value"]].apply(process_data,axis=1)
        df.index=df["name"]
        decoded   = df["data"]
        undecoded = df["value"]
        return decoded.to_dict(), undecoded.to_dict()
    
    def print_embed(self,hook,df,title=None):
        
        webhook     = Webhook.from_url(hook, adapter=RequestsWebhookAdapter())    
        columns     = df.columns
        valuesList  = df.values.tolist()
        e = Embed()
        for idx, columnName in enumerate(columns):
            string = ''
            for value in valuesList:
                string =  string +  "\n" + str(value[idx])
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