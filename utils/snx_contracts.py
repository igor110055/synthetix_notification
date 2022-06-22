import json
import pandas as pd
import requests
from utils.utility import get_abi

class SnxContracts():

    def __init__(self,conf):
            
        self.conf    = conf
        self.get_contracts_df()
        self.abis    = dict()
                                                           
    def get_contracts_df(self):
        self.snxContractsDict = dict()
        for net in self.conf["snxContracts"].keys():
            html = self.conf["snxContracts"][net]
            contractTree=json.loads(requests.get(html).text)
            contractsDF = pd.DataFrame()
            for version in contractTree:
                df=pd.DataFrame(contractTree[version]["contracts"]).T
                contractsDF = pd.concat([contractsDF,df],axis=0)
            self.snxContractsDict[net] = contractsDF
            
    def get_all_contract_addresses(self,targetContractName,network):        
        addressList = list()
        data = requests.get(self.conf["snxContracts"][network]).json()    
        for version in data.keys():
            for contractName in data[version]["contracts"].keys():
                if targetContractName == contractName :
                    addressList.append(data[version]["contracts"][targetContractName]["address"])
        return addressList
    
    def get_snx_contract(self,contractName,w3,network):
        address  = self.get_address(contractName=contractName,network=network)
        if self.abis.get(network+"_"+address,0)==0:
            self.abis[network+"_"+address] = get_abi(conf=self.conf, address=address, network=network)
        abi = self.abis[network+"_"+address]
        return w3.eth.contract(address=address,abi=abi)

    def get_address(self,contractName,network):
        contractsDF = self.snxContractsDict[network].query("index==@contractName and status=='current'")
        return contractsDF["address"].values[0]