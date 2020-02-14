#Feature extraction from available API

#todo: 1) refactor holiday check and add?
#      2) set lifecyle on athena query bucket (temp location), instead of currenet method of delete files after copy.

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
RETRY_COUNT = 5 #number of retries

#------------------ queries run on below athena table-----------------------
#create table if required below athena db *refrence incase tabld is dropped
'''
CREATE EXTERNAL TABLE IF NOT EXISTS historic_incidents_db.daily_summaries_partition(
`date` string,
`weekday` string,
'incidentcount` int,
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


import boto3
import json
import datetime
from datetime import date, timedelta
from botocore.vendored import requests
import urllib2
import json

from athena_lambda import *

def athena_next_24_hrs_past_24_hrs():
    '''
    create a rolling 48hour window for dashboard statistics every 24hours (set at 1145)
    
    '''
    #set current date 
    dt=datetime.datetime.utcnow() + datetime.timedelta(hours=10)
    today = datetime.datetime.strftime(dt,"%Y%m%d") #string yyyymmdd
    day_of_week = dt.strftime('%A')
    
    #set tomorrows date 
    dt_24hours=dt - datetime.timedelta(hours=24) #24 hour previous 
    yesterday_day_of_week = dt_24hours.strftime('%A')
    
    sel_stat_1 = 1
    sel_stat_2 = 2
    col_wkday = 'weekday'    
    col_inc = 'incidentcount' 
    col_hm = 'hhmm_utcplus10' 
    
    #anthena query
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
    
    query = "SELECT %s,%s,%d as select_order, " \
        "min (%s) as minimum_incidents," \
        "approx_percentile(%s, 0.25) as appx_Q1_incidents," \
        "AVG (%s) as average_incidents," \
        "approx_percentile(%s, 0.75) as appx_Q3_incidents," \
        "max (%s) as maximum_incidents " \
        "FROM %s.%s where %s = '%s' " \
        "GROUP BY %s,%s" \
        " UNION " \
        "SELECT %s,%s,%d as select_order, " \
        "min (%s) as minimum_incidents," \
        "approx_percentile(%s, 0.25) as appx_Q1_incidents," \
        "AVG (%s) as average_incidents," \
        "approx_percentile(%s, 0.75) as appx_Q3_incidents," \
        "max (%s) as maximum_incidents " \
        "FROM %s.%s where %s = '%s' " \
        "GROUP BY %s,%s ORDER BY select_order,%s;"  \
        % (col_wkday, col_hm,sel_stat_1,col_inc,col_inc,col_inc,col_inc,col_inc,
        DATABASE, TABLE, col_wkday, day_of_week,col_wkday,col_hm,
        col_wkday, col_hm,sel_stat_2,col_inc,col_inc,col_inc,col_inc,col_inc,
        DATABASE, TABLE, col_wkday, yesterday_day_of_week,col_wkday,col_hm,col_hm) 
    
    #calls athena query
    results,query_execution_id = athena_qry(query,DATABASE,S3_OUTPUT,RETRY_COUNT)
 
    #using id move csv file to S3 curated bucket for typical weekday and weekend
    client_s3 = boto3.resource('s3')
    # Copy anthena query to curated csv for frontend rendering - html,D3
    client_s3.Object(bucketname_routes, "stat/rolling_48_hour_window.csv")    .copy_from(CopySource= filepath_incidents_statistic_write + "qry/"+ query_execution_id + ".csv")
    
    # Delete the former object A
    client_s3.Object(bucketname_routes, "stat/qry/"+ query_execution_id + ".csv").delete()
    client_s3.Object(bucketname_routes, "stat/qry/"+ query_execution_id + ".csv.metadata").delete()
    #set lifecycle policy in bucket subfolder - month exipiry (28-32 files)
    print 'SUCCESSFUL - athena_next_24_hrs_past_24_hrs'  
    return


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
            % (col_wkday, col_hm, col_inc,col_inc,col_inc,col_inc,col_inc,
            DATABASE, TABLE, col_wkday, day_of_week,col_wkday,col_hm,col_hm) #stat query
    
    #calls athena query
    results,query_execution_id = athena_qry(query,DATABASE,S3_OUTPUT,RETRY_COUNT)

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


def lambda_handler(event, context):
    
    try:
        athena_typical_day_qry()
        athena_next_24_hrs_past_24_hrs()
    except Exception, err:
        print err
        return
    
    print 'SUCCESSFUL COMPLETED - lamabda'
    
    return 
