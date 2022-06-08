import time
import requests
import yaml
import web3 as w3
from web3 import Web3
from web3.middleware import geth_poa_middleware

def parse_config(path):
    with open(path, 'r') as stream:
        return  yaml.load(stream, Loader=yaml.FullLoader)

def get_w3(conf,network):
    rpc = conf["nodes"][network]["http"]
    web3 = w3.Web3(w3.HTTPProvider(rpc))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    return web3

def get_abi(conf,address,network):
    headers      = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    url = conf["etherscan"]["abi"][network].format(address)
    while True:
        try:
            result = requests.get(url,headers=headers).json()
            if result["status"] != '0':
                return result["result"]
            else:
                print("error seen with abi fetch, trying again")
                time.spleep(3)
                continue
        except:
            print("error seen with abi fetch, trying again")
            time.sleep(3)

def process_data(data):
    dtype,value = data
    if dtype =='bytes32':
        return Web3.toText(value).replace("\x00",'')
    elif dtype =='address':
        return value
    elif dtype == 'uint256' or dtype == 'int256':
        return round(value/1e18,4)
    else:
        return value
            