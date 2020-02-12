#Feature extraction from available API

#todo: refractor and excpetions methods for athena methods(e.g def athena_qry)

# Based on location of incident ('In progres') return proximity HERE flow network
# Save S3 location
bucketname_routes="public-test-road"
filepath_incidents_read_prefix_key="stat/"
filepath_incidents_statistic_write = "public-test-road/stat/"

# Athena parametres
DATABASE = 'incidents'
TABLE = 'daily_summaries_dashboard'
S3_OUTPUT = 's3://public-test-road/stat/qry'
S3_BUCKET = 'qry'
# number of retries
RETRY_COUNT = 5

import time
import boto3
import logging
import json
import datetime
from datetime import date, timedelta

from botocore.vendored import requests
import urllib2
import json



def lambda_handler(event, context):
  
    #set current date and yesterday
    dt=datetime.datetime.utcnow() + datetime.timedelta(hours=10)
    today = datetime.datetime.strftime(dt,"%Y%m%d") #string yyyymmdd
    day_of_week = dt.strftime('%A')


    #if holiday - Store Holiday

#------------------ run athena queries -----------------------
    #send to athena and query all csvs
    
    #create table if required below athena db
    '''
    CREATE EXTERNAL TABLE IF NOT EXISTS historic_incidents_db.daily_summaries_partition(
      `date` string,
      `weekday` string,
      `incidentcount` int,
      `crashcount` int 
    ) PARTITIONED BY (
      `yymmdd_utcplus10` string 
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    WITH SERDEPROPERTIES (
      'serialization.format' = ',',
      'field.delim' = ','
    ) LOCATION 's3://public-test-road/stat/bytime/'
    TBLPROPERTIES ('has_encrypted_data'='false')
    '''
    
    #anthena query
    '''
    # daily stats
    SELECT weekday, hhmm_utcplus10,
    min (incidentcount) as "minimum_incidents",
    approx_percentile(incidentcount, 0.25) as "appx_Q1_incidents",
    AVG (incidentcount) as "average_incidents",
    approx_percentile(incidentcount, 0.75) as "appx_Q3_incidents",                      
    max (incidentcount) as "maximum_incidents"
    FROM "incidents"."daily_summaries_dashboard" where weekday = 'Monday' 
    GROUP BY weekday,hhmm_utcplus10 ORDER BY hhmm_utcplus10 ASC
    
    '''
    
    #build above athena query
    #query = "SELECT * FROM %s.%s where %s = '%s';" % (DATABASE, TABLE, col_wkday, keyword) #simple query (test)
    col_wkday = 'weekday'    
    col_inc = 'incidentcount' 
    col_hm = 'hhmm_utcplus10' 
    
    query = "SELECT %s,%s," \
            "min (%s) as minimum_incidents," \
            "approx_percentile(%s, 0.25) as appx_Q1_incidents," \
            "AVG (%s) as average_incidents," \
            "approx_percentile(%s, 0.75) as appx_Q3_incidents," \
            "max (%s) as maximum_incidents " \
            "FROM %s.%s where %s = '%s' " \
            "GROUP BY %s,%s ORDER BY %s ASC;" \
            % (col_wkday, col_hm, col_inc,col_inc,col_inc,col_inc,col_inc, DATABASE, TABLE, col_wkday, day_of_week,col_wkday,col_hm,col_hm) #stat query
    

    #query and save to s3
    # athena client
    logging.basicConfig(filename='athena.log',level=logging.INFO)
    
    client = boto3.client('athena')
    # Execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': S3_OUTPUT,
        }
    )
    
    #get query execution id - kdy in s3
    query_execution_id = response['QueryExecutionId']

    # get execution status - awating succesfull response
    # todo: rewrite below to wait based on time rather than current loop 
    for i in range(1, 1 + RETRY_COUNT):

        # get query execution
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']

        if query_execution_status == 'SUCCEEDED':
            print("STATUS:" + query_execution_status)
            break

        if query_execution_status == 'FAILED':
            raise Exception("STATUS:" + query_execution_status)

        else:
            print("STATUS:" + query_execution_status)
            time.sleep(i)
    else:
        client.stop_query_execution(QueryExecutionId=query_execution_id)
        raise Exception('TIME OVER')
 
    # get query results
    result = client.get_query_results(QueryExecutionId=query_execution_id)
    print query_execution_id

    # get data
    
    
    #print result
    print len(result['ResultSet']['Rows'])

    #using id move csv file to S3 curated bucket for typical weekday and weekend
    if len(result['ResultSet']['Rows']) == 2:
        email = result['ResultSet']['Rows'][1]['Data'][1]['VarCharValue']
        print email
    else:
        return None
    print '-----'
    
#------------------ iterate through csv -----------------------
    #get incidents from S3 Bucket, only in progress.
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketname_routes)
    #read all csv files
    for obj in bucket.objects.filter(Delimiter='/', Prefix=filepath_incidents_read_prefix_key):
        #print obj.key
        filename = str(obj.key)
        if filename != filepath_incidents_read_prefix_key: #don't include empty key
            print filename
            
        #process file by day of week e.g 'Wednesday'
    return

    org_incCsv = pd.read_csv(s3_client.get_object(Bucket=bucketname_routes, Key=filepath_incidents_read)['Body'])
    

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
