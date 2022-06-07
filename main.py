#!/usr/bin/env python3
from utils.utility import parse_config
from utils.controller import Controller
import argparse
conf       = parse_config(r"config/conf.yaml")
socketConf = parse_config(r"config/socketConf.yaml")
#%%Arg Parse
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='launches the process to gather minters data')

    parser.add_argument("-t",
                        type=str,
                        required=True,
                        choices=["prod","stage"],
                        help="Please choose between prod and stage")

    args = parser.parse_args()
    
    
    control = Controller(conf[args.t],socketConf)
    control.trigger_controller()