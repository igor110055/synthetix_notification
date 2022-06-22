from utils.utility import get_w3

class Loans():
    
    def __init__(self,conf):
        self.conf      = conf
        self.setup_loan_dict()

    def setup_loan_dict(self):
        loanDict = dict()
        #for saving loan contract to loan ids to ccy
        for network in self.conf["nodes"].keys():
            loanDict[network] = {'CollateralEth':{},
                                 'CollateralErc20':{},
                                 'CollateralShort':{}}
        self.loanDict = loanDict

    def get_loan_ccy(self,loanId,userAddress,network,contractName):
        if network == 'optimism':
            if not loanId in self.loanDict[network][contractName].keys():
                w3 = get_w3(self.conf,network)
                contract = self.get_snx_contract(contractName,w3,network)
                data = contract.functions.loans(loanId).call()
                self.loanDict[network][contractName][loanId] = w3.toText(data[3]).replace("\x00","")        
            return self.loanDict[network][contractName][loanId]
        elif network == 'ethreum':
            splitName         = contractName.split('Collateral')
            stateContractName = 'CollateralState'+splitName[1]
            if not loanId in self.loanDict[network][contractName].keys():
                w3 = get_w3(self.conf,network)
                stateContract     = self.get_snx_contract(stateContractName,w3,network)
                data              = stateContract.functions.getLoan(w3.toChecksumAddress(userAddress),loanId).call()
                self.loanDict[network][contractName][loanId] = w3.toText(data[3]).replace("\x00","")        
            return self.loanDict[network][contractName][loanId]
    