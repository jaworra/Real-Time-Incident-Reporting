#Feature extraction from available API

#todo: 1) refactor holiday and next 24 hour result
#      2) cloud wat     

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
import json
import datetime
from datetime import date, timedelta
from botocore.vendored import requests
import urllib2
import json



def athena_next_24_hrs_past_24_hrs():
    '''
    create a rolling 48hour window for dashboard statistics every 24hours
    
    '''
    #set current date 
    dt=datetime.datetime.utcnow() + datetime.timedelta(hours=10)
    today = datetime.datetime.strftime(dt,"%Y%m%d") #string yyyymmdd
    day_of_week = dt.strftime('%A')
    
    #set tomorrows date 
    dt_24hours=dt + datetime.timedelta(hours=24) #24 hour previous 
    tomorrows_day_of_week = dt_24hours.strftime('%A')
    
    print day_of_week
    print tomorrows_day_of_week
    
    return


    '''
SELECT weekday, hhmm_utcplus10, 1 as select_order,
    min (incidentcount) as "minimum_incidents",
    approx_percentile(incidentcount, 0.25) as "appx_Q1_incidents",
    AVG (incidentcount) as "average_incidents",
    approx_percentile(incidentcount, 0.75) as "appx_Q3_incidents",                      
    max (incidentcount) as "maximum_incidents"
    FROM "incidents"."daily_summaries_dashboard" where weekday = 'Monday' 
    GROUP BY weekday,hhmm_utcplus10
    UNION 
SELECT weekday, hhmm_utcplus10, 2 as select_order,
    min (incidentcount) as "minimum_incidents",
    approx_percentile(incidentcount, 0.25) as "appx_Q1_incidents",
    AVG (incidentcount) as "average_incidents",
    approx_percentile(incidentcount, 0.75) as "appx_Q3_incidents",                      
    max (incidentcount) as "maximum_incidents"
    FROM "incidents"."daily_summaries_dashboard" where weekday = 'Tuesday' 
    GROUP BY weekday,hhmm_utcplus10 Order by select_order,hhmm_utcplus10
    '''
    
    
    
    


def athena_typical_day_qry():
    '''
    create typical day metricsfrom partiioned csv files use athena to query every 24 hours
    to calcuate statistics of incidents assumes table setup in athena
    
    sample output
    weekday	hhmm_utcplus10	minimum_incidents	appx_Q1_incidents	average_incidents	appx_Q3_incidents	maximum_incidents
    Wednesday	0	36	49	58.55	68	89
    Wednesday	15	36	49	57.9	66	90
    Wednesday	30	41	52	59.22222222	63	91
    Wednesday	45	40	49	57.23529412	61	89
    Wednesday	100	38	48	56.14285714	60	86

    '''
 
    #set current date and yesterday
    dt=datetime.datetime.utcnow() + datetime.timedelta(hours=10)
    today = datetime.datetime.strftime(dt,"%Y%m%d") #string yyyymmdd
    day_of_week = dt.strftime('%A')
    
    #Need to do qa check at 9ma the following code returns the right day (update lambda test_jworrall_incidents_current_delta line 455)
    #print dt.today().strftime('%A')

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

    # get execution status - awating succesfull response, todo: rewrite below to wait based on time rather than current loop 
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

    #using id move csv file to S3 curated bucket for typical weekday and weekend
    client_s3 = boto3.resource('s3')
    # Copy anthena query to curated csv for frontend rendering - html,D3
    client_s3.Object(bucketname_routes,  "stat/typical_" + day_of_week.lower() + ".csv").copy_from(CopySource= filepath_incidents_statistic_write + "qry/"+ query_execution_id + ".csv")
    
    # Delete the former object A
    client_s3.Object(bucketname_routes, "stat/qry/"+ query_execution_id + ".csv").delete()
    client_s3.Object(bucketname_routes, "stat/qry/"+ query_execution_id + ".csv.metadata").delete()
    #set lifecycle policy in bucket subfolder - month exipiry (28-32 files)
    print 'SUCCESSFUL - athena_typical_day_qry'
    return 


#query 24hour ahead...
    
def lambda_handler(event, context):
    
    athena_next_24_hrs_past_24_hrs()
    return

    try:
        athena_typical_day_qry()
    except Exception, err:
        print err
    
    print 'SUCCESSFUL - lamabda'
    return 
