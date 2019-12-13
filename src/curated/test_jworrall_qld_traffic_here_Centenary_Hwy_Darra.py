#Comparing similar HERE and Streams links speed.
## Can we see when a QLD traffic proposed roadworks starts and ends from timeseries HERE data.
## Using streams as true - compare quality of here information prior to writing buisness rules.

###  Chosen road workds site: Centenary Highway - Darra
# Works will commence in early-August 2019 and occur between 8pm and 5am, Sunday to Thursday,
# with the majority of work being on the road network. 


# Out of hours works will also be undertaken across the site to reduce impacts to the road network and ensure the safety of workers. 
# - Speed will be reduced on Sumners Road to 40 kilometres per hour, and for both directions of the Centenary Motorway to 80 kilometres per hour in the active worksite areas. 
# - Traffic control and signage will be in place as required. Please allow for extra time when planning your journey. 
# - To ensure motorist and public safety, please follow all traffic and project related signage.

out_bucket_name ="public-test-road"
key_output_file_1= r'results/hypothesis2/livelinks_here_31862.csv'
key_output_file_2 = r'results/hypothesis2/livelinks_streams_8484273.csv'
key_output_file_3 = r'results/hypothesis2/livelinks_streams_8484287.csv'
key_output_file_4 = r'results/hypothesis2/livelinks_streams_average.csv'

#HERE secrets
app_id = 'Q56YtQZX205BCqVWB4UT'
app_code = 'RkLmGtCor_WSh79Xg4egzA'

#STREAM secrets & setpayload
url ='https://api.dtmr.staging.data.streams.com.au'
ser_SIMSRecent  = url + "/traffic/v1/link/csv"
headers_txt = {'Content-type': 'application/csv','x-api-key': 'USoLc2B9De86v9QHy5ahcaXkXJd8Fq0a7MCwTI2V'}


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
    
    incidentId ='CentenaryHighway'
    incidentCord = '-27.555910,152.940246'
    
    # #setup dataframe
    dfcols = ['id','linkId','name','avSpeed','jamF','direc','cords']
    dfHere = pd.DataFrame(columns = dfcols)

    #configure session request API
    starttime = time.time()
    urlsession = requests.session()
    prox = "0.01"# "20" #proximity in metres 
    
    #configure payload
    url = "https://traffic.api.here.com/traffic/6.2/flow.json?app_id=" + app_id + "&app_code=" + app_code
    url +="&prox="+incidentCord+","+prox+"&responseattributes=sh,fc&units=metric"
    #send request
    response = requests.get(url, timeout=600)    
    response = response.content
    #clean up
    urlsession.close()    
    
    
    #break if no return
    if response !="": #only process return values
    
        #process json return for output
        try:
            r=json.loads(response)   
            for el1 in r['RWS']:
                for el2 in el1['RW']:
                        for el3 in el2['FIS']: #Road level
                            for el4 in el3['FI']: #flow information extract here at link level
                                linRd = el4['TMC'].get('DE').replace("'","") #get rid of ' i.e "O'keefe Street" to "Okeefe Street"
                                linRd_pk =el4['TMC'].get('PC')
                                flowInfoDirection = el4['TMC'].get('QD') 
                                #print(linRd)
                                flowInfoSpeed = el4['CF'][0].get('SU') #Speed (based on UNITS) not capped by speed limit
                                flowInfoJam = el4['CF'][0].get('JF') #The number between 0.0 and 10.0 indicating the expected quality of travel. When there is a road closure, the Jam Factor will be 10. As the number approaches 10.0 the quality of travel is getting worse. -1.0 indicates that a Jam Factor could not be calculated
                                flowInfoCon =  el4['CF'][0].get('CN') #Confidence, an indication of how the speed was determined. -1.0 road closed. 1.0=100% 0.7-100% Historical Usually a value between .7 and 1.0
                                for el5 in el4['SHP']: #get shape file
                                    cordStr = el5['value']
                                    dfHere.loc[len(dfHere)] = [incidentId,linRd_pk,linRd, flowInfoSpeed,flowInfoJam,flowInfoDirection,cordStr]
        except Exception as ex:
            print(str(response))
            raise ex       

    #return only a single link from response json
    link_of_interest=dfHere.loc[(dfHere['linkId'] == 31862) & (dfHere['name'] == 'Sumners Road') & (dfHere['direc'] == '+') ] 

    #extract speed
    link_of_interest_avspeed = round(link_of_interest['avSpeed'].mean(),1)
    
    #extract jamFactor
    link_of_interest_jamfactor = round(link_of_interest['jamF'].mean(),1)
    
    #set time variable
    link_time =date.strftime(datetime.datetime.utcnow() + datetime.timedelta(hours=10),'%Y-%m-%d %H:%M') 

    # #extract coordinates if required...
    # here_locations = [] 
    # # iterate over rows with iterrows()
    # for index, row in link_of_interest.head().iterrows():
    #      # access data using column names
    #     cord_string='['+str(row['cords'])+']'
    #     cord_string = cord_string.replace(' -','],[-').replace("'","")
    #     cord_string =eval(cord_string)
    #     here_locations.append(cord_string)

    # read existing csv file
    s3_client = boto3.client('s3')
    #here_31862 LINK
    org_incCsv = pd.read_csv(s3_client.get_object(Bucket=out_bucket_name, Key=key_output_file_1)['Body'])

    # append to file 
    org_incCsv.loc[len(org_incCsv)] = [str(31862),str(link_of_interest_avspeed),str(link_of_interest_jamfactor),str(link_time)]

    # Send out to S3
    tmpfp=r'/tmp/combine_csv.csv' #we have 300MB of storage under /tmp
    with open(tmpfp, 'w') as h:
        h.write('id,speed,jamf,time'+ '\n')
        for index, row in org_incCsv.iterrows():
            lineCsv = str(row['id'])+','+str(row['speed'])+','+str(row['jamf'])+','+str(row['time']) + '\n'  
            h.write(str(lineCsv))
    s3_client.upload_file(tmpfp,Bucket=out_bucket_name,Key=key_output_file_1)


    ### This section calls Streams transmax loop

    #sensor 8484273
    payload_ser_aggDet = {
            'ids':8484273
              }   
    
    response = requests.get(ser_SIMSRecent,params=payload_ser_aggDet, headers=headers_txt, timeout=600)
    decoded_content = response.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')

    my_list = list(cr)
    #return speed if a valid record
    if int(my_list[1][0]) > 0:
        id_8484273_speed = int(my_list[4][4])
        id_8484273_time = date.strftime(datetime.datetime.strptime(my_list[4][1][:-1],'%Y-%m-%dT%H:%M:%S.%f')+timedelta(hours = 10), '%Y-%m-%d %H:%M')
        
    #sensor 8484287
    payload_ser_aggDet = {
            'ids':8484287
              }   
    response = requests.get(ser_SIMSRecent,params=payload_ser_aggDet, headers=headers_txt, timeout=600)
    decoded_content = response.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    my_list = list(cr)
    #return speed if a valid record
    if int(my_list[1][0]) > 0:
        id_8484287_speed = int(my_list[4][4]) 
        id_8484287_time = date.strftime(datetime.datetime.strptime(my_list[4][1][:-1],'%Y-%m-%dT%H:%M:%S.%f')+timedelta(hours = 10), '%Y-%m-%d %H:%M')
        
    #get average values from two sensors
    if (id_8484273_speed > 0) & (id_8484287_speed > 0):
        average_speed =(id_8484273_speed +id_8484287_speed)/2
    else:
        average_speed =0 

    # read and send out to S3 - 1st loop 8484273.
    tmpfp=r'/tmp/loops_csv.csv' #we have 300MB of storage under /tmp
    #read
    org_incCsv = pd.read_csv(s3_client.get_object(Bucket=out_bucket_name, Key=key_output_file_2)['Body'])
    # append  
    org_incCsv.loc[len(org_incCsv)] = [str(8484273),str(id_8484273_speed),str(id_8484273_time)]
    # write to file 
    with open(tmpfp, 'w') as h:
        h.write('id,speed,time'+ '\n')
        for index, row in org_incCsv.iterrows():
            lineCsv = str(row['id'])+','+str(row['speed'])+','+str(row['time']) + '\n'  
            h.write(str(lineCsv))
    s3_client.upload_file(tmpfp,Bucket=out_bucket_name,Key=key_output_file_2)

 
     # Send out to S3 - loop 8484287
    # key3 = r'hypothesis2/livelinks_streams_8487428.csv' #wrong
    #read
    org_incCsv = pd.read_csv(s3_client.get_object(Bucket=out_bucket_name, Key=key_output_file_3)['Body'])
    # append  
    org_incCsv.loc[len(org_incCsv)] = [str(8484287),str(id_8484287_speed),str(id_8484287_time)]
    with open(tmpfp, 'w') as h:
        h.write('id,speed,time'+ '\n')
        for index, row in org_incCsv.iterrows():
            lineCsv = str(row['id'])+','+str(row['speed'])+','+str(row['time']) + '\n'  
            h.write(str(lineCsv))
    s3_client.upload_file(tmpfp,Bucket=out_bucket_name,Key=key_output_file_3)   
                

    # Send out to S3
    #key4 = r'hypothesis2/livelinks_streams_average.csv'
    #read
    org_incCsv = pd.read_csv(s3_client.get_object(Bucket=out_bucket_name, Key=key_output_file_4)['Body'])
    # append  
    org_incCsv.loc[len(org_incCsv)] = ['aveage',str(average_speed),str(id_8484287_time)]
    with open(tmpfp, 'w') as h:
        h.write('id,speed,time'+ '\n')
        for index, row in org_incCsv.iterrows():
            lineCsv = str(row['id'])+','+str(row['speed'])+','+str(row['time']) + '\n'  
            h.write(str(lineCsv))
    s3_client.upload_file(tmpfp,Bucket=out_bucket_name,Key=key_output_file_4) 

    return
