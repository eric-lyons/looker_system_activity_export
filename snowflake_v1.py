import looker_sdk
from numpy import False_
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from looker_sdk import models
import csv

def create_snowflake_database(datum):
     # Creates a connection to a snowflake database
     # This creates a warehouse, schema, database and table.
     # For more information on the snowflake connector library, please visit
     # https://docs.snowflake.com/en/user-guide/python-connector-example.html
     # The pandas connector can be used, but that will rewrite an existing table.
            
            #Args:

            #The only args required here is datum, which is the csv output from the Looker
            #System Activity query. For security purposes, you could make the snowflake credentials
            #enviromental variables.
   
    connector = snowflake.connector.connect(
        user='USERNAME',
        password='PASSWORD',
        account='ACCOUNTID'
    )
    cs = connector.cursor()
    try:
        print('creating wh')
        sql = "CREATE WAREHOUSE IF NOT EXISTS system_activity_warehouse"
        cs.execute(sql)

        print('creating db...')
        sql = "CREATE DATABASE IF NOT EXISTS looker_system_activity_clone"
        cs.execute(sql)

        print('setting db...')
        sql = "USE DATABASE looker_system_activity_clone"
        cs.execute(sql)

        print('creating schema...')
        sql = "CREATE SCHEMA IF NOT EXISTS i__looker"
        cs.execute(sql)
        print('complete!')

        ## Setting use now
        sql = "USE WAREHOUSE system_activity_warehouse"
        cs.execute(sql)

        sql = "USE DATABASE looker_system_activity_clone"
        cs.execute(sql)

        sql = "USE SCHEMA i__looker"
        cs.execute(sql)

        sql = ("CREATE OR REPLACE TABLE I__LOOKER_SCHEDULED_PLAN"
        "(scheduled_plan_id integer, scheduled_plan_cron_schedule string, scheduled_plan_name string, user_email string, scheduled_plan_destination_format string, scheduled_plan_destination_addresses string)" )
        cs.execute(sql)


        df = pd.read_csv('datum.csv', sep=',', index_col=False)

        ## Headers need to be identical to what is in snowflake
        df = df.rename(columns={'Scheduled Plan ID':'SCHEDULED_PLAN_ID',
                                    'Scheduled Plan Cron Schedule':'SCHEDULED_PLAN_CRON_SCHEDULE', 
                                    'Scheduled Plan Name':'SCHEDULED_PLAN_NAME', 
                                    'User Email':'USER_EMAIL', 
                                    'Scheduled Plan Destination Format':'SCHEDULED_PLAN_DESTINATION_FORMAT', 
                                    'Scheduled Plan Destination Addresses':'SCHEDULED_PLAN_DESTINATION_ADDRESSES'
                                    })
    
        ### loop through with an insert statement

        for row in df.index:
            # Removing trailing or leading quotes 
            title = df['SCHEDULED_PLAN_NAME'][row]
            title = title.replace("'", "")
            insert = ("INSERT INTO I__LOOKER_SCHEDULED_PLAN (SCHEDULED_PLAN_ID, SCHEDULED_PLAN_CRON_SCHEDULE, SCHEDULED_PLAN_NAME, USER_EMAIL, SCHEDULED_PLAN_DESTINATION_FORMAT, SCHEDULED_PLAN_DESTINATION_ADDRESSES)"
            "VALUES (" +  str(df['SCHEDULED_PLAN_ID'][row]) + ", '" + str(df['SCHEDULED_PLAN_CRON_SCHEDULE'][row]) + "', '" + title + "', '" +  str(df['USER_EMAIL'][row]) +  "', '" + str(df['SCHEDULED_PLAN_DESTINATION_FORMAT'][row]) +  "', '"  + str(df['SCHEDULED_PLAN_DESTINATION_ADDRESSES'][row]) +  "')")
            print(insert)
            cs.execute(insert)
        
       
    finally:
        cs.close()
    connector.close()



def get_data_from_system_activity():
    # This function uses the inline query endpoint and queries data from sytem activity. 
    # The filter is hard coded to pull data from yesterday and the row limit is set to -1 which means it should
    # be a full result set. This is under the guise this script would be run everyday and would insert the data from
    # looker into snowflake with a 24 hour lag. 

        # Args:
           # No args required for this function. The initialization of the SDK assumes there is an .ini file with
           # the proper looker credentials. 
  
    sdk = looker_sdk.init40()
    query_config = models.WriteQuery(
        model="system__activity",
        view="scheduled_plan",
        fields=[
            "scheduled_plan.id",
            "scheduled_plan.cron_schedule",
            "scheduled_plan.name",
            "user.email",
            "scheduled_plan_destination.format",
            "scheduled_plan.destination_addresses"],
            limit='100',
            filters = {"scheduled_job_stage.completed_date":"yesterday"},
            sorts=["scheduled_plan.id"]
        )
    datum = sdk.run_inline_query(
            result_format='csv',
            body=query_config
            )
    file = open('datum.csv', 'w')
    file.write(datum)
    file.close
    return datum


def main():
    datum = get_data_from_system_activity()
    create_snowflake_database(datum)

main()


