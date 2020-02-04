#Feature extraction from available API


# Based on location of incident ('In progres') return proximity HERE flow network
# Save S3 location
bucketname_routes="public-test-road"
filepath_incidents_read_prefix_key="stat/"
filepath_incidents_statistic_write = "public-test-road/stat/"

# Athena parametres
DATABASE = 'historic_incidents_db'
TABLE = 'daily_summaries_partition'
# S3 constant
S3_OUTPUT = 's3://public-test-road/stat/qry'
S3_BUCKET = 'qry'
# number of retries
RETRY_COUNT = 5
# query constant
COLUMN = 'weekday'    


import time
import boto3
import json
import datetime
from datetime import date, timedelta

from botocore.vendored import requests
import urllib2
import json


def lambda_handler(event, context):
  
  
    
    #set current date and yesterday
    dt=datetime.datetime.utcnow() + datetime.timedelta(hours=10)
    today = datetime.datetime.strftime(dt,"%Y%m%d") #string today
    dayOfWeek = dt.today().strftime('%A')

    print today
    print dayOfWeek
    
    #if holiday - Store Holiday

#------------------ run athena queries -----------------------
    #send to athena and query all csvs
    
    #table in below athena db
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
    
    
    #approx_percentile(traveltime_minutes, 0.00) as "percentile_000",
    
    
    
    #https://dev.classmethod.jp/cloud/run-amazon-athenas-query-with-aws-lambda/
    # get keyword
    #keyword = event['name']
    keyword = dayOfWeek
    
    
    # created query
    query = "SELECT * FROM %s.%s where %s = '%s';" % (DATABASE, TABLE, COLUMN, keyword)
    #SELECT * FROM "historic_incidents_db"."daily_summaries" where weekday = 'Friday' limit 100;
    #SELECT * FROM "historic_incidents_db"."daily_summaries" limit 100;
    
    COLUMN2 = "yymmdd_utcplus10"
    keyword = "1600"
    keyword
    #SELECT * FROM "historic_incidents_db"."daily_summaries_partition" where yymmdd_utcplus10='1600' and date = '20200131' limit 10
    query = "SELECT * FROM %s.%s where %s = '%s' and ;" % (DATABASE, TABLE, COLUMN2, keyword)
    
    #run sql to bring back only the last day.
    #SELECT * FROM "historic_incidents_db"."daily_summaries_partition" where weekday = 'Friday'
    #SELECT weekday, AVG (incidentcount) FROM "historic_incidents_db"."daily_summaries_partition" where weekday = 'Friday' GROUP BY weekday,yymmdd_utcplus10;
    #approx_percentile(traveltime_minutes, 0.00) as "percentile_000",
    
    '''
    SELECT weekday, AVG (incidentcount) as "average_incidents",
                    AVG (crashcount) as "average_crashes",
                    yymmdd_utcplus10
                    FROM "historic_incidents_db"."daily_summaries_partition" where weekday = 'Monday' 
                    GROUP BY weekday,yymmdd_utcplus10 ORDER BY yymmdd_utcplus10 ASC
                    
                    
    SELECT weekday, AVG (incidentcount) as "average_incidents",
                    approx_percentile(incidentcount, 0.50) as "percentile_050 check",
                    AVG (crashcount) as "average_crashes",                
                    yymmdd_utcplus10
                    FROM "historic_incidents_db"."daily_summaries_partition" where weekday = 'Monday' 
                    GROUP BY weekday,yymmdd_utcplus10 ORDER BY yymmdd_utcplus10 ASC
                    
                    
    SELECT weekday, min (incidentcount) as "minimum_incidents",
                approx_percentile(incidentcount, 0.25) as "appx_Q1_incidents",
                AVG (incidentcount) as "average_incidents",
                approx_percentile(incidentcount, 0.75) as "appx_Q3_incidents",                      
                max (incidentcount) as "maximum_incidents",       
                yymmdd_utcplus10
                FROM "historic_incidents_db"."daily_summaries_partition" where weekday = 'Monday' 
                GROUP BY weekday,yymmdd_utcplus10 ORDER BY yymmdd_utcplus10 ASC
                    
    '''
    
    #do a count in query 
    
    # athena client
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
    
    # get query execution id
    query_execution_id = response['QueryExecutionId']
    print(query_execution_id)
    
 
  # get execution status
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
    print(result)

    # get data
    if len(result['ResultSet']['Rows']) == 2:

        email = result['ResultSet']['Rows'][1]['Data'][1]['VarCharValue']

        return email

    else:
        return None
 
    return
        
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
