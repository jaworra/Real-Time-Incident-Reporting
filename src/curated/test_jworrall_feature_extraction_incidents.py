#Feature extraction from available API
# Based on location of incident ('In progres') return proximity HERE flow network
# Call once a day to check for incidents and related variables 
# Save S3 location

#global variables 
bucketname_routes="public-test-road"
filepath_incidents_read="data/curated/live_incidents.csv"
filepath_incidents_write = "data/curated/feature_extraction_incidents.json"
filepath_links_HERE_write = "data/curated/here_links_flow_priority_archive.json"

#for HERE api Global Tokens
#app_id = ''
#app_code = ''

import json
import datetime
import boto3
from zipfile import ZipFile
import io
import os
import sys
import shutil
import time
import botocore
import re  
import math
#from datetime import datetime,date, timedelta

from combinesets import current_waze, closes_pt, current_weather,current_holidays,current_here_links_flow
from utmconversion import from_latlon

#from athena import *
#from glue import addpartitionifcan

from collections import OrderedDict, defaultdict
from datetime import date, timedelta
from io import StringIO

try:
    from botocore.vendored import requests
except ImportError: #have to get it in AWS Lambda from here instead
    import requests #'pip install requests', if you are running locally, if this errors

extrapackagesins3_bucketname="tmr-mpi-ttdash-dev-temp"
extrapackagesins3_filepath = "20190101_py.zip" #contains pandas, pyarrow, shapely etc

#==============================================================================================
def extrapackagesins3_load(): #copies a ZIP file of useful python packages from S3 
                    #and unzips them into /tmp/pyfiles, 
                    #and adjusts Python's path to let them then be 'import'ed
                    #The .zip file can be created in an EC2 AMI instance, by
                    #creating a subfolder, going into it, then doing 
                    # pip install <packagename> -t .
                    #for each package, then
                    # zip -r <zipfilenametocreate> *
                    #then finally:
                    # aws s3 cp <zipfilename> s3://<bucketname>/<destfilename>
    print("Preparing extra Python packages for use, from S3 bucket "+extrapackagesins3_bucketname+', path '+extrapackagesins3_filepath)
    extrapackagesins3_filename=os.path.basename(extrapackagesins3_filepath)
    if not os.path.isfile('/tmp/'+extrapackagesins3_filename): #just in case, avoid unnecessary downloading
        print('Downloading file from S3')
        s3 = boto3.resource('s3') #ref: http://boto3.readthedocs.io/en/latest/reference/services/s3.html
        bucket=s3.Bucket(extrapackagesins3_bucketname)
        bucket.download_file(extrapackagesins3_filepath, "/tmp/"+extrapackagesins3_filename)
        print('File downloaded ok')
    else:
        print('Not downloading file from S3, we seem to already have it')
    
    destdir='/tmp/'+extrapackagesins3_filename+'.extracted'
    extractedokflagfile=destdir+'/.extractedok'
    if not os.path.isfile(extractedokflagfile): #just in case it failed and this is a retry
        print("Unzipping file from S3 into "+destdir)
        if os.path.isdir(destdir):
            shutil.rmtree(destdir) #just in case, clean up after a partial extraction/whatever last time
        os.mkdir(destdir)
        with ZipFile('/tmp/'+extrapackagesins3_filename) as zf:
            zf.extractall(destdir) 
        with io.open(extractedokflagfile,'wt') as f: #write out only once finished
            f.write(u'ok')
        print("Unzipped ok")
    else:
        print("No need to unzip file from S3, we've completed that before")
        
    sys.path.insert(0, destdir) #tell Python to 'import' from here now
    
    print("Ready to support 'import' of the extra packages, which are:")
    print("-------------")
    for subfolder in [dI for dI in os.listdir(destdir) if os.path.isdir(os.path.join(destdir,dI))]:
        if not subfolder.endswith('.dist-info'):
            print(subfolder)
    print("-------------")

#################################################################################
#                     INSTANCE LOAD TIME
#################################################################################

#do now, at instance load
extrapackagesins3_load() 


#these imports are supported by 'extrapackagesins3_load'
import csv
import uuid
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


from shapely import geometry as shapelygeom
from pandas.io.json import json_normalize #take this out

from datetime import date, timedelta


def changeCoordsStr(latLong):
    """
    takes dictionary - change format from [lat,long] to [long,lat]
    {'value': ['-28.14909,153.4798 -28.14903,153.4798 -28.14867,153.47978 -28.14825,153.47979 -28.14783,153.47982 -28.14718,153.47987 -28.14671,153.47987 '], 'FC': 1} to
    '[153.4798,-28.14909],[153.4798,-28.14903],[153.47978,-28.14867],[153.47979,-28.14825],[153.47982,-28.14783],[153.47987,-28.14718],[153.47987,-28.14671]''   
    """

    tmpStr = str(latLong.get('value')).replace("'","")
    tmpStr = tmpStr.replace(" -2","|-2").replace("[","").replace("]","").strip()

    tmpStr= tmpStr.split("|")
    cordsSwap="["
    for index in range(len(tmpStr)):
        lat, lon = tmpStr[index].split(",")
        cordsSwap += "["+lon+","+lat+"],"
    cordsSwap = cordsSwap[:-1]+"]" #remove the last character ',' and close ']'
    cordsSwap = cordsSwap.replace("u","") #additional clean up in aws envirn
    return cordsSwap


def lambda_handler(event, context):
    
    startTime = time.time() #Start Time
    
    #get incidents from S3 Bucket, only in progress.
    s3_client = boto3.client('s3')
    print("Reading input csv defining the in progress incidients from s3://"+bucketname_routes+"/"+filepath_incidents_read)
    org_incCsv = pd.read_csv(s3_client.get_object(Bucket=bucketname_routes, Key=filepath_incidents_read)['Body'])
    incCsv = org_incCsv[['id','lat','lng','status','blockageType']]
    incCsv = incCsv[incCsv.status == 'In Progress']
    
    #sample data frame
    '''
               id        lat         lng       status
    0    16707135 -27.900924  153.285993  In Progress
    1    16707118 -27.122562  152.793635  In Progress
    2    16707092 -26.624883  152.912590  In Progress
    3    16707089 -27.820994  153.290409  In Progress
    5    16707056 -27.960379  153.345259  In Progress
    '''   
    
    #print incCsv
    incCsv = org_incCsv[incCsv.blockageType == 'Unknown']   #FIX THIS UP LATER!!
    #incCsv = incCsv.iloc[0]
    
    #print incCsv
    incCsv_dict = incCsv.set_index('id').T.to_dict('list') #dataframe to dictionary
    
    here_prox = 100 #serach area around incident in meters
    number_of_incidents = 3 #Of the number of incients serach k

    here_flow_dict = current_here_links_flow(incCsv_dict,here_prox,number_of_incidents) #flow of here links,

    #Retrun dictionary from methonds
    last_line = len(here_flow_dict)
    last_line = last_line - 5 #bug here - go back 5 rows (see current_here_links_flow method)

    #Send out to S3
    tmpfp=r"/tmp/geojson" #we have 300MB of storage under /tmp
    with open("/tmp/temp.csv", 'w') as h:
        h.write("pathdata=[")
    
        for k in here_flow_dict:
            here_link_str = here_flow_dict[k]
            #print str(k)
            if here_link_str is not None and k < last_line:
                h.write("{route: '%s',incident: %s,start_sam: 0,end_sam: 100000,jamF: %s,speed: '%s',coords: %s},\n" % (str(here_link_str['name']),str(here_link_str['id']),str(here_link_str['jamF']),str(here_link_str['avSpeed']),str(here_link_str['cords'])))
            elif k == last_line:
                h.write("{routeXXXXX: '%s',incident: %s,start_sam: 0,end_sam: 100000,jamF: %s,speed: '%s',coords: %s}]\n" % (str(here_link_str['name']),str(here_link_str['id']),str(here_link_str['jamF']),str(here_link_str['avSpeed']),str(here_link_str['cords'])))

    s3 = boto3.resource('s3')
    outbucket=s3.Bucket(bucketname_routes)
    outbucket.upload_file("/tmp/temp.csv", filepath_links_HERE_write) 
     
     
    #Send out link?           
    print 'sent outs'
    
    #Send out to S3
    geojson = "pathdata=["    
    tmpfp=r"/tmp/geojson" #we have 300MB of storage under /tmp
    with open("/tmp/temp.csv", 'w') as h:
        h.write("pathdata=[")
        for i,(index, row) in enumerate(dfHere.iterrows()):
            if i != len(dfHere) - 1:
                h.write("{route: '%s',incident: %s,start_sam: 0,end_sam: 100000,jamF: %s,speed: '%s',coords: %s},\n" % (str(row['name']),str(row['id']),str(row['jamF']),str(row['avSpeed']),str(row['cords'])))
            else:
                h.write("{route: '%s',incident: %s,start_sam: 0,end_sam: 100000,jamF: %s,speed: '%s',coords: %s}]" % (str(row['name']),str(row['id']),str(row['jamF']),str(row['avSpeed']),str(row['cords'])))
                
    s3 = boto3.resource('s3')
    outbucket=s3.Bucket(bucketname_routes)
    outbucket.upload_file("/tmp/temp.csv", filepath_routes_HERE_write) 
    
    #Program execuation time
    print("--- %s seconds ---" % (time.time() - startTime))
        
    #debug
    #date = '20190419' #known holiday
    #current date
    date=datetime.datetime.utcnow() + datetime.timedelta(hours=10)
    date = date.strftime('%Y%m%d')    
    is_a_holiday, holiday_name = current_holidays(date)  
    
    #loop through through each incidnet and for 'waze' affected routes.
    waze_alert_with_attributes =[] #take this out -no need for initialisation?
    waze_alert_list,waze_alert_with_attributes = current_waze()  #Produce list of waze incidents in qld
    
    ###---
    #waze_alert_with_attributes - gets sent out to s3
    #waze_alert_list - for correlation with streams incident

    #write out to S3 Bucket waze_alert_with_attributes
    with open("/tmp/waze.csv", 'w') as h:
        h.write('alert,lat,lng'+ '\n')
        i = 1
        size_of_list = len(waze_alert_with_attributes)
        while i < size_of_list:
            lineCsv = str(waze_alert_with_attributes[i][0])+','+str(waze_alert_with_attributes[i][1])+','+str(waze_alert_with_attributes[i][2]) + '\n'    
            h.write(str(lineCsv))
            i += 1
            
    s3 = boto3.resource('s3')
    outbucket=s3.Bucket(bucketname_routes)
    outbucket.upload_file("/tmp/waze.csv", filepath_incidents_WAZE_write) 


    dfcols = ['id','lat','lng','status','blockageType','classification','loggedTime','wazeCorrelation','temp','weather','holiday']#,'weatherConditions']
    dfCorrelation = pd.DataFrame(columns = dfcols)
    for index, row in org_incCsv.iterrows():
        
        #coordiantes format to MGA 56
        incident_cord_parsed = (float(row['lat']) , float(row['lng']))
        limiteasting, limitnorthing, _ , _ = from_latlon(latitude=float(row['lat']) , longitude=float(row['lng']), force_zone_number=56)
        incident_cord_parsed = (limitnorthing ,limiteasting)# reassign lat long to north east     

        #waze proximity
        closes_waze_incident_to_streams_incident = closes_pt(waze_alert_list,incident_cord_parsed)  
        dist_from_streams_to_waze = closes_waze_incident_to_streams_incident[2]

        #condition return value  - need to add attribute data of value.
        if dist_from_streams_to_waze < 100:
            waze_proximity = "within proxity - "+dist_from_streams_to_waze
        else:
            waze_proximity = "None"
            
        #1)Current weather conditions of block incident
        temperature, weather = current_weather(str(row['lat']),str(row['lng']))
        
        #build point data result values.
        #dfCorrelation.loc[len(dfCorrelation)] = [str(row['id']),str(row['status']),str(row['blockageType']),str(row['classification']),str(row['loggedTime']),waze_proximity,temperature, weather,is_a_holiday]
        dfCorrelation.loc[len(dfCorrelation)] = [str(row['id']),str(row['lat']),str(row['lng']),str(row['status']),str(row['blockageType']),str(row['classification']),str(row['loggedTime']),waze_proximity,temperature, weather,is_a_holiday]


    #Send out to S3
    session = boto3.Session()
    s3_client = session.client('s3')
    tmpfp=r'/tmp/combine_csv.csv' #we have 300MB of storage under /tmp
    with open(tmpfp, 'w') as h:
        h.write('id,lat,lng,status,blockageType,classification,loggedTime,wazeCorrelation,temp,weather,holiday'+ '\n')
        for index, row in dfCorrelation.iterrows():
            lineCsv = str(row['id'])+','+str(row['lat'])+','+str(row['lng'])+','+str(row['status'])+','+str(row['blockageType'])+','+str(row['classification'])+','+str(row['loggedTime'])+','+str(waze_proximity)+','+str(temperature)+','+str(weather)+','+str(is_a_holiday) + '\n'    
            h.write(str(lineCsv))
    s3_client.upload_file(tmpfp,Bucket=bucketname_routes,Key=filepath_incidents_write)
                
    return


  

