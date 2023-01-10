#!/usr/bin/env python
# coding: utf-8


import json
import argparse
from datetime import datetime


from snowflake.snowpark.session import Session

import logging        
#NEEDED FOR MUTEX
logger = logging.getLogger("snowflake.snowpark._internal.server_connection")
logger.setLevel(logging.FATAL)

def get_and_execute(contract_type, start_date, end_date):
    # ### Create snowpark session

    with open('snowflake-creds.json') as f:
        data = json.load(f)
        username = data['username']
        password = data['password']
        accountname = data['account']
        role = data['role']
        database = data['database']
        schema = data['schema']
        warehouse = data['warehouse']


    connection_parameters = {
	    "account": accountname,
	    "user": username,
	    "password": password,
	    "role": role,
	    "database": database,
	    "schema": schema,
	    "warehouse": warehouse
	}
    
    session = Session.builder.configs(connection_parameters).create()
    
    #execute business logic

    session.close()


if __name__=='__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--contract-type', '-ct', type=str, default='A')
    p.add_argument('--start-date', '-s', type=str, default='2022-11-03 00:00:00')
    p.add_argument('--end-date', '-e', type=str, default='2022-11-04 00:00:00')
    args = p.parse_args()
    contract_type = args.contract_type.replace('"', '')
    start_date = args.start_date.replace('"', '')
    end_date = args.end_date.replace('"', '')
    get_and_execute(contract_type, start_date, end_date)




