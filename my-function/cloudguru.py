import pandas as pd
from datetime import datetime
import sys
#from data_transformation import *
import boto3
import base64
from botocore.exceptions import ClientError
import psycopg2
import numpy as np
import json

from data_transformation import df_data_transform




def lambda_handler(event, context):
# Variables Initialization
 NYTset = 'NYT DATASET'
 JohnHOpkinsset = 'JohnHopkins DATASET'
 NYTdataURL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
 JohnHopkinsURL = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv?opt_id=oeu1602173839233r0.3109590658815089'
 
 table_name = 'covid19_table_test'
 DT = datetime.now().strftime('%Y-%m-%d')

 def reading_csv_files(dataset_URL, DATASET_NAME):
    try :
        res = pd.read_csv(dataset_URL,header=0, delimiter=',',index_col = False)
        return res

    except:
        print("Error while reading CSV data:" + DATASET_NAME + "from Git" )
        sys.exit()
        
 def error_notifictaion(msg):
    sns = boto3.client('sns', region_name = 'us-west-2')
    try:
        #print(msg)
        sns_publish_response = sns.publish(
        TargetArn=str('arn:aws:sns:us-west-2:574876523720:cloudguru_notification'), Message=msg)
    except:
        print("Error while publishing SNS notification")
        sys.exit()

 def get_secret():
    secret_name = "rds_postgres_covid"
    region_name = "us-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':

            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':

            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':

            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':

            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':

            raise e
    else:

        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            #print(secret)
            return json.loads(secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret


 def data_quality_check(df1,df2):

     if (df1.dtypes['date'] != np.object or df2.dtypes['Date'] != np.object):
        msg = "Date Column contains unexpected or malformed input"
        error_notifictaion(msg)
        sys.exit()

     if df1.dtypes['cases'] != np.int64 :
        msg = "Data Column(Cases) contains unexpected or malformed input for NYT Dataset"
        error_notifictaion(msg)
        sys.exit()

     if df1.dtypes['deaths'] != np.int64:

       msg = "Data Column(Deaths) contains unexpected or malformed input for NYT Dataset"
       error_notifictaion(msg)
       sys.exit()
     '''
     if df2.dtypes['Recovered'] != np.int64:
        msg = "Data Column(Recovered) contains unexpected or malformed input for JohnHopkins Dataset"
        error_notifictaion(msg)
        sys.exit()
    '''
 sns = boto3.client('sns', region_name = 'us-west-2')

 response = get_secret()
 db_username = response['username']
 db_password = response['password']
 db_host = response['host']
 db_dbname = response['dbname']
 #print(response)

 NYTData =reading_csv_files(NYTdataURL,NYTset)
 JOhnHopkinsData = reading_csv_files(JohnHopkinsURL, JohnHOpkinsset)

 data_quality_check(NYTData,JOhnHopkinsData)

 print('Establishing connection to to RDS Postgres Database')

#Postgres_conn = psycopg2.connect("host=covid19-challenge.chmkxpe5o969.us-west-2.rds.amazonaws.com dbname=covid19_test user=postgres password=abcd1234 port=5432")
 Postgres_conn =psycopg2.connect(dbname=db_dbname, user=db_username, password=db_password, host=db_host,port=5432)

 table_check_query = "select to_regclass('" + table_name + "')"

 cursor = Postgres_conn.cursor()
 cursor.execute(table_check_query, ['{}.{}'.format('schema', 'table')])
 table_exists = cursor.fetchall()[0][0]

 if table_exists == 'None':
    table_exists = 'False'
 else:
    table_exists = 'True'


 if table_exists == 'False':


    create_table_query = "CREATE TABLE " + table_name + "(Date date PRIMARY KEY, Cases integer,Deaths integer,Recovered integer);"
    cursor = Postgres_conn.cursor()
    cursor.execute(create_table_query)


 cursor = Postgres_conn.cursor()
 record_check_query = "select count(*) from " + table_name

 cursor.execute(record_check_query)
 rec_count = cursor.fetchall()[0][0]
 print("Number of Records present in the Database:" + str(rec_count))
 if rec_count == 0:
    initial_load_flag = "True"
    max_date = 'None'
 else:
    initial_load_flag = "False"
    #print(initial_load_flag)
    max_date_query = "select max(Date) from " + table_name
    cursor = Postgres_conn.cursor()

    #print(max_date)
    cursor.execute(max_date_query)
    max_date = cursor.fetchall()[0][0]
    #print(max_date)

    max_date = datetime.strftime(max_date,'%Y-%m-%d')

 Merged_Dataset = df_data_transform(NYTData,JOhnHopkinsData,'date','inner',initial_load_flag,max_date)

 Merged_Datset_final = Merged_Dataset.rename(columns={"date": "Date","cases":"Cases","deaths":"Deaths"})


 for index in Merged_Datset_final.index:
    #print(Merged_Datset_final['Date'][index], Merged_Datset_final['Cases'][index])

    query = """INSERT into """+table_name+""" (date, cases, deaths,recovered) values({},{},{},{})""".format(
        "'" + str(datetime.date(Merged_Datset_final['Date'][index])) + "'", Merged_Datset_final['Cases'][index], Merged_Datset_final['Deaths'][index],
        int(Merged_Datset_final['Recovered'][index]))
    #print(query)
    cursor = Postgres_conn.cursor()
    cursor.execute(query)
    Postgres_conn.commit()

 num_updated = len(Merged_Datset_final.index)
 msg = "ETL Load completed for: " + DT + "\n Number of Rows Updated: " + str(num_updated)
 error_notifictaion(msg)
