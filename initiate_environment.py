#!/usr/bin/env python
# coding: utf-8

import json


from snowflake.snowpark.session import Session
from snowflake.snowpark.types import (
    StringType,
    StructType,
    StructField,
    IntegerType,
    DecimalType,
    BooleanType
)


def create_tables(dummy_table_name:str, tracker_table_name:str):
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
    #create dummy table
    dummy_table_schema = StructType([StructField("DUMMY", IntegerType())])
    dummy_table_snowdf = session.create_dataframe([[None]*len(dummy_table_schema.names)], schema=dummy_table_schema)
    dummy_table_snowdf.write.save_as_table(dummy_table_name, mode="overwrite", table_type="transient")
    #create mapping tracker table
    tracker_table_schema = StructType([StructField("START_TIMESTAMP", DecimalType()),
                                                            StructField("END_TIMESTAMP", DecimalType()),
                                                            StructField("IS_CURRENTLY_REFRESHING", BooleanType()),
                                                            StructField("RESULT_TABLE_NAME", StringType())
                                                ])
    tracker_table_snowdf = session.create_dataframe([[None]*len(tracker_table_schema.names)], schema=tracker_table_schema).na.drop()
    tracker_table_snowdf.write.save_as_table(tracker_table_name, mode="overwrite")
    

if __name__=='__main__':
    create_tables("DUMMY_MUTEX_TABLE", "MAPPING_TRACKER")


