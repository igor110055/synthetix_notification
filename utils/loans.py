from utils.utility import get_w3

class Loans():
    
    def __init__(self,conf):
        self.conf      = conf
        self.shortDict = {}
    
    def get_short_currency(self,loanId,network):
        if loanId in self.shortDict.keys():
            return self.shortDict[loanId]
        w3 = get_w3(self.conf,network)
        contract = self.get_snx_contract('CollateralShort',w3,network)
        data = contract.functions.loans(loanId).call()
        self.shortDict[loanId] = w3.toText(data[3]).replace("\x00","")        
        return self.shortDict[loanId]
    
        
    
    
    