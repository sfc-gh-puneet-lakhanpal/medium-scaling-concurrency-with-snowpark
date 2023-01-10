#!/usr/bin/env python
# coding: utf-8
from tenacity import retry, stop, wait, retry_if_exception_type, retry_if_exception_message
from typing import List, Tuple, Callable, Any
import pandas as pd
RETRY_COUNT = 10000
WAIT_EXP_MULT = 1
WAIT_EXP_MAX = 10  # 10 seconds
WAIT_FIXED = 10 # 10 seconds
from datetime import datetime
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.dataframe import DataFrame
import time
import uuid

class A:
    @classmethod
    def obtain_start_and_end_timestamp_from_constraint(self, constraint) -> list:
        start = constraint.start
        end = constraint.end
        if isinstance(start, datetime):
            start = pd.Timestamp(start).value
        if isinstance(end, datetime):
            end = pd.Timestamp(end).value
        else:
            raise ValueError("Unsupported constraint type {constraint}")
        return [start, end]
    
    @classmethod
    def get_unique_table_name(self, table_name:str, given_uuid: str = None) -> str:
        """Generates a unique table name to stage data"""
        if given_uuid is None:
            given_uuid = uuid.uuid4()
        date = datetime.now().strftime("%Y%m%d")
        given_uuid = str(given_uuid).replace("-", "_")
        return f"{table_name}__{date}__{given_uuid}".rstrip()

    def log_retry_attempt_number(retry_state):
        print(f"Retrying: {retry_state.attempt_number}...")

    
    @classmethod
    @retry(
        retry=(retry_if_exception_type(SnowparkSQLException) & (retry_if_exception_message(match=r".*: 000625.*") | 
                                                           retry_if_exception_message(match=r".*: 000626.*") |
                                                           retry_if_exception_message(match=r".*: 000627.*"))
              ),
        stop=stop.stop_after_attempt(RETRY_COUNT),
        wait=wait.wait_fixed(WAIT_FIXED),
        after=log_retry_attempt_number
    )
    def acquire_lock_and_initiate(self, tracker_table_name: str, snow_df:DataFrame, constraint) -> DataFrame:
        session = session.context.get_active_session()
        start, end = self.obtain_start_and_end_timestamp_from_constraint(constraint)
        #mutex implementation to acquire lock on tracker_table_name table
        session.sql("ROLLBACK;").collect()
        session.sql("BEGIN;").collect()
        session.sql("UPDATE DUMMY_MUTEX_TABLE SET DUMMY=NULL where IFF(DUMMY IS NULL, FALSE, TRUE);").collect()
        time.sleep(5)
        existing_snowdf = session.table(tracker_table_name).filter(f" START_TIMESTAMP <= {start} and START_TIMESTAMP <= {end} and END_TIMESTAMP >= {start} and END_TIMESTAMP >= {end} ")
        count_in_tracker_table = existing_snowdf.count()
        is_table_newly_created = False
        if count_in_tracker_table == 0:
            unique_table_name = self.get_unique_table_name("INTERIM")
            data = {
                'START_TIMESTAMP': [start],
                'END_TIMESTAMP': [end],
                'IS_CURRENTLY_REFRESHING': [True],
                'RESULT_TABLE_NAME': unique_table_name
            }
            new_snowdf = session.create_dataframe(pd.DataFrame.from_dict(data))
            new_snowdf.write.save_as_table(tracker_table_name, mode="append", table_type = "temporary")
            # Begin business logic

            existing_tracker_table_name = self.perform_business_logic(snow_df, unique_table_name, "transient")

            # End business logic

            session.sql(f"update {tracker_table_name} set IS_CURRENTLY_REFRESHING=False where START_TIMESTAMP={start} and END_TIMESTAMP={end} and RESULT_TABLE_NAME='{unique_table_name}'").collect()
            session.sql("COMMIT;").collect()
            is_table_newly_created = True
        else:
            session.sql("COMMIT;").collect()
            current_refreshing_status = True
            while current_refreshing_status == True:
                current_refreshing_status = existing_snowdf.select("IS_CURRENTLY_REFRESHING").distinct().collect()[0][0]
                if current_refreshing_status == True:
                    time.sleep(5)
                continue
            existing_tracker_table_name = existing_snowdf.select("RESULT_TABLE_NAME").distinct().collect()[0][0]
            is_table_newly_created = False
        if is_table_newly_created:
            return session.table(existing_tracker_table_name)
        else:
            return session.table(existing_tracker_table_name).filter(f" RECORD_TIME >= {start} and RECORD_TIME < {end}")
    
    @classmethod
    def validate_tracker_and_initiate_with_retry(self, tracker_table_name: str, snow_df:DataFrame, constraint) -> DataFrame:
        #mutex implementation to acquire lock on tracker_table_name table
        try:
            result_snowdf = self.acquire_lock_and_initiate(tracker_table_name, snow_df, constraint)
            return result_snowdf
        except:
            session = session.context.get_active_session()
            session.sql("COMMIT;").collect()
            print("Error in initiating")