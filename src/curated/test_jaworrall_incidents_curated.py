#John Worrall
#Data Insights and solutions
#Loads the incidents using transmax streams API for Dashboard visualisation

# Checks recent Incidents 'In progress', compares previously stored data returns delta
# Relates the above to possible affected bus routes - checks RDS (postgres) stored bus routes
# Write output in specific format for table/graphical object for end user dashboard (javascript libraries - D3/Deck.gl etc)
# Update summary statistics on incidents frequency and other factors.

##TODO: Refractor code to funciton
#      Optimise memory

#secrets
app_id = ''
# #Chenage the below details to reader!!!
# db_host=''
# db_port=
# db_name=''
# db_user=''
# db_password=''

#below is the readonly replica
db_port=
db_name=''
db_password=''
db_user=''
db_host='' #readonly replica

#output files
bucket_name= 'public-test-road'
key_dashboard = 'data/curated/summarised_incidents.csv'
key_list_view = 'data/curated/live_incidents_dash.csv' 
key_map_view = 'data/curated/live_incidents.csv'

from postgres_db import *

#Priorites upgrades
#Next steps - migrate 
#CSV to JSON (better formating integration with deck.gl)
#Try methods to catch excemptions on api being down. 

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


def lambda_handler(event, context):

    #check out streams API
    #https://www.data.streams.com.au/apidocs-staging-tmr/#operation/AlgorithmsCurated_ListQueues
    url= "https://api.dtmr.staging.data.streams.com.au"
    
    #construct request
    #Auth header
    headers_txt = {
        'Content-type': 'application/json',
         'x-api-key': app_id
         }
    
    #Get SIMS Recent Incidents
    #Service is descriped as recent incidents (in progress or 48 hours)
    #But this service returns incidents logged in 2017 (treating this as complete extract)
    
    #Process below calls api twice - 1) returns a count on total records 2) returns latest records based on total - x (this is a constant)
    ser_SIMSRecent  = url + "/Incidents/v1/sims"
    payload_ser_SIMSRecent = {
        'size': 5,
        }    
    starttime = time.time()
    urlsession = requests.session()
    response = requests.get(ser_SIMSRecent,params=payload_ser_SIMSRecent, headers=headers_txt, timeout=600)
    jdata= response.json()
    
    #reset payload based total size minus chosen records per day
    numRec = jdata.get('totalFound')

    #Resend with new payload calc
    ser_SIMSRecent  = url + "/Incidents/v1/sims"
    payload_ser_SIMSRecent = {
        'size': 500,
        'from' : numRec-500
        }    
    starttime = time.time()
    urlsession = requests.session()
    response = requests.get(ser_SIMSRecent,params=payload_ser_SIMSRecent, headers=headers_txt, timeout=600)
    jdata= response.json()        
    #data = response.content
    
    #set current date and yesterday
    dt=datetime.datetime.utcnow() + datetime.timedelta(hours=10)
    dty = datetime.datetime.strftime(dt- timedelta(days=1),"%Y-%m-%d") #yesterday 
    today = datetime.datetime.strftime(dt,"%Y%m%d") #string today
    dt_24hours=dt - datetime.timedelta(hours=24) #24 hour previous 
    

    #process parse json
    featC = jdata.get('featureCollection')
    features = featC.get('features')
    csvFeat = []
    dfcols = ['id','coordinates','status','direction','way','classification','suburb','linkId','intersectionId','delay','assignedTo','blockageType','location','startTime','loggedTime','endTime','type','road','region','severityCategory']
    df = pd.DataFrame(columns = dfcols)
    data = []
    for item in features:
        #Uncoment below for assigning variables.....
        featId = item.get("id")
        featId = featId[:featId.index(':')]  # From 16547289:28:streams.metro.tmr.its to 16547289
        geo =  item.get("geometry")
        if geo is not None:
            cord = str(geo.get("coordinates"))
            #cord=cord.replace('[', '').replace(']', '')
        else:
            cord = ""
        prop = item.get("properties")
        typ = prop.get("type")
        status = prop.get("status")
        assTo = prop.get("assignedTo")
        logg = prop.get("loggedTime")
        logg = datetime.datetime.strptime(logg,'%Y-%m-%dT%H:%M:%S.%fZ')+datetime.timedelta(hours=10) #convert to UTC10plus
        stT = prop.get("startTime")                    
        endT= prop.get("endTime")
        sb = prop.get("suburb")
        loc = prop.get("location")      
        rd = prop.get("road") 
        rd = str(rd).title() #change Uppercase - too much shouting on Roads
        #below - appends suburb to road
        # if sb is not None: #add the suburb to Road - for dashboard locality
        #     rd = str(rd) + " / "+str(sb)
        #     rd = rd.title() #change Uppercase - too much shouting on Roads
        way = prop.get("way")        
        di = prop.get("direction")      
        intId = prop.get("intersectionId")    
        lId = prop.get("linkId")
        dela = prop.get("delay")        
        bloT = prop.get("blockageType")        
        clas = prop.get("classification")
        reg = prop.get("region")
        sevC = prop.get("severityCategory")

        
        #datetime.datetime.strftime(logg,"%Y-%m-%dT%H:%M:%S")
        if datetime.datetime.strftime(logg,"%Y-%m-%dT%H:%M:%S") > datetime.datetime.strftime(dt_24hours,"%Y-%m-%dT%H:%M:%S"): #Compare 24 hour rolling period
            csvFeat.extend([item]) #Same format
            data.append([featId,cord,status,di,way,clas,sb,lId,intId,dela,assTo,bloT,loc,stT,logg,endT,typ,rd,reg,sevC]) #dataframe -> json. De-nesting, allowing for json consumption in athena ="Collated"
            #print logg
        
        # if logg[:10] == today: #if logged date matches today *** FIX this for 24hours 
        #     csvFeat.extend([item]) #Same format
        #     data.append([featId,cord,status,di,way,clas,sb,lId,intId,dela,assTo,bloT,loc,stT,logg,endT,typ,rd]) #dataframe -> json. De-nesting, allowing for json consumption in athena ="Collated"
           
    df = pd.DataFrame(data, columns=dfcols)
    df = df[::-1] #reverese order -> for home page
    #print df_alert
        
    sum_data = df.loc[df['status'] == 'In Progress']

    #write out to bucket
    print "-successful output-"
    # print logg[12:19]+" "+logg[:10]
    # return
    
  
#==================================Compare and add data====================================================================
#==================================Instead of rewrite====================================================================
#if the bus not affected "None" else provide ID

#look for unique value in dataframe 1 (Read csv), then delete this record
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=key_list_view)
    old_data = pd.read_csv(obj['Body'])
    
    #comparing dataframes - keep only matching records of old records 
    #first check if there is a difference - no difference exit...
    
    #checking new data for only in progress, convert id type to integer, reindex 
    #replace the bottom with ....   
    #new_data = df[df['status'].str.contains("In Progress")]
    
    new_data = df.loc[df['status'] == 'In Progress']
    crash_data = new_data.loc[new_data['type'] == 'Crash']
    
    total_inc = len(new_data)# Total in progress 
    total_crsh = len(crash_data)#Total crash
    
    new_data["id"] = new_data["id"].astype('int64')
    new_data.index = range(len(new_data))
    old_data["id"] = old_data["id"].astype('int64')

    #compare if Incidents from the API - has changed see 
    if new_data['id'].equals(old_data['id']):
        print "No changes - Exit system. Latest records..."
        print new_data[0:5]['id']
        return 
    else:
        print "Things have changed - continue with append."
        
#-------------------TAKE THIS OUT-------------------    
        new_data['Bus Routes Affected'] = "" #initilise new column
        validRet=0 #set boolean (false bus route returned)
        #new_data = new_data[1:7]  #for debugging
        
        new_data=new_data[['id','type','road','blockageType','classification','loggedTime','coordinates','Bus Routes Affected','delay','suburb','region','severityCategory']] #Select out columns to print
        #'region','severityCategory'
        #set columns type for comparison
        new_data['loggedTime']= new_data['loggedTime'].astype(object)
        new_data['loggedTime']= new_data['loggedTime'].dt.strftime("%H:%M:%S  %d/%m")

        #set 'id' cloumn to query on and return busroutes affected, from previous call.
        #No need to redo poximiity/temporaly analysis on all incidents - just do the deltas
        d = old_data.set_index('id')['Bus Routes Affected'].to_dict()
        new_data['Bus Routes Affected']=new_data['id'].map(d)


        #need to update this !! the busus affected are don at logged time not including as they currently time!!! 
        #Loop through dataframe to insert Bus affected by incidents
        conn=open_database_connection_with_autocommit_off(db_name,db_user,db_password,db_host,db_port,max_millisecs_connection_will_be_open_for=5000)
        print "-+++++++-"
        #print new_data[0:5]['Bus Routes Affected']
        #print new_data[0:5]['id']
        for index in new_data.index:
            if str(new_data['Bus Routes Affected'][index]) == "nan":  #Where bus route has no value - look for bus routes affected by incidents
                print '------Check new incidents to see if impacts a bus------ '
                print ("df[" + str(index) + "]['Bus Routes Affected']=" + str(new_data['Bus Routes Affected'][index]))
                
                locIncident = str(new_data['coordinates'][index]).replace(', ', ' ').replace('[', '').replace(']', '').replace(',,', '')
                #locIncident = "152.966744 -27.627813" #debug
                
                timeIncident = "'2018-06-26 "+str(new_data['loggedTime'][index])[:8]+"'" #static date "Tuesday" dynamic time
                #timeIncident = "'2018-06-26 07:12:5'" #debug
        
                locProximty = "'100'"    
                anlQuery= "SELECT route_id FROM gtfs_20180626_BusRoute where stop_departuredatetime " \
                          "<= "+timeIncident+" and nextstop_arrivaldatetime >= "+timeIncident+" and ST_Distance_Sphere(ST_GeomFromText" \
                          "('POINT("+locIncident+")',4326),geom)< "+locProximty+" LIMIT 1;" #only records less than 10metres and within time
                          
                print anlQuery
                
                if locIncident == '': #if there is no coordinate from the incidents.
                    busAffected="No location of Incident"    
                    validRet=1
                    print "no loction given on incident"

                else:   #There is coordiantes to query to postgres database (spatial and temporal analysis)
                    for row in fetchall_asyielderofdict(anlQuery, conn):    
                        print "Found!!! - Bus Route affected by incident"
                        print row
                        busAffected=row.get('route_id')
                        validRet=1
              
                #this below proces should be fixed - need to change if statement to no rows returned!!    
                if validRet==0: #No rows return from query - no bus affected
                    busAffected="No Buses Affected" 
                    print "no loction given on incident"
    
                #assign value
                new_data['Bus Routes Affected'][index]=busAffected
                validRet=0 
            
            # else:            
            #     print 'No proccessing Done - using original valuess'
            
        print "************"

#-------------------TAKE THIS OUT-------------------        

    print "-output section-" 
    #print total_inc
    #look for unique value in dataframe 2 (API extract), apped record and search from associated bus route

#========================Writes out to visualiation/map=======================================================
    #If no dataframe maybe delete the source

    session = boto3.Session()
    s3_client = session.client('s3')
    
    #writes out csv live Visualisation (deck.gl) - but only the records with coordinates (able to be displayed)
    tmNew=r"/tmp/tmp.csv"
    with open(tmNew,"w") as f:
        f.write('id,lng,lat,status,blockageType,classification,loggedTime'+ '\n') #headers
        for index, row in df.iterrows():
            #print(row['id'], row['coordinates'])
            lineCsv = str(row['id'])+","
            lineCsv+= str(row['coordinates']).replace('[', '').replace(']', '').replace(', -', ',-')+","
            #lineCsv+=str(row['status'])+","+str(row['blockageType'])+","+str(row['classification'])+","+str(row['loggedTime'])+'\n' #Instead of logged time - change formatt to HH:MM:SS YYYY-MM-DD
            #lineCsv+=str(row['status'])+","+str(row['blockageType'])+","+str(row['classification'])+","+logg[12:19]+" "+logg[:10]+'\n' #Instead of logged time - change formatt to HH:MM:SS YYYY-MM-DD
            lineCsv+=str(row['status'])+","+str(row['blockageType'])+","+str(row['classification'])+","+str(row['loggedTime'])[11:19]+" "+str(row['loggedTime'])[8:10] +"-"+ str(row['loggedTime'])[5:7] +"-"+ str(row['loggedTime'])[:4] + '\n'            
            #print lineCsv
            if not str(row['coordinates']) == '':
                #print "no coordinates!!"
                f.write(lineCsv)
    s3_client.upload_file(tmNew,Bucket=bucket_name,Key=key_map_view)
    #return
#===========================writes out to table/dashboard=============================================
    #Opt 1) - write out table without BusRoutes affected
    #Opt 2) - Adds BusRoutes affected
    
    Opt=2
    tmNew=r"/tmp/tmp2.csv"    

    if Opt ==1:
        #df=new_data#check
        #writes out csv for home page (table)
        with open(tmNew,"w") as f:
            f.write('id,type,road,blockageType,classification,loggedTime'+ '\n') #headers
            for index, row in df.iterrows():
                lineCsv = str(row['id'])+"," +str(row['type'])+","                
                lineCsv = str(row['id'])+"," +str(row['type'])+","
                lineCsv+=str(row['road'])+","+str(row['blockageType'])+","+str(row['classification'])+","+str(row['loggedTime'])[11:19]+"  "+str(row['loggedTime'])[8:10] + '\n'    
                if str(row['status']) == 'In Progress': #only bring back In Progress records
                    f.write(lineCsv)
        s3_client.upload_file(tmNew,Bucket=bucket_name,Key=key_list_view)
        
    #old format
    #ID | TYPE | BLOCKAGE TYPE | CLASSIFICATION | ROAD | LOGGEDTIME |	BUS ROUTES AFFECTED
    
    #new format    
    #SIMS Id | Event type | Location | District | Start time | Impact| Bus routes potentially affected        
    #id | Type (classification)  |	Road, Suburb location  | region	 | Starttime  |	Delay blockageType severityCategory

    elif Opt ==2:
        #print new_data
        #print list(new_data)
        #print strnew_data['loggedTime']
        #"09:27:03 07/08'
        #return
        df=new_data#check
        #writes out csv for home page (table)
        with open(tmNew,"w") as f:
            #f.write('id,type,blockage Type,classification,road,loggedTime,Bus Routes Affected'+ '\n') #headers
            f.write('id,Event Type,Location,District,logged Time,Impact,Bus Routes Affected'+ '\n') #headers            
            for index, row in df.iterrows():
                lineCsv = str(row['id'])+"," +str(row['type'])+" ("+str(row['classification'])+"),"
                #print str(row['suburb'])
                lineCsv += str(row['road'])+" ("+str(row['suburb'])+"),"+str(row['region'])+","+str(row['loggedTime'])[0:8]+","
                lineCsv += str(row['delay'])+" "+str(row['blockageType'])+" ("+str(row['severityCategory'])+"),"+str(row['Bus Routes Affected']) +'\n'
                
                #lineCsv += str(row['delay'])+" "+str(row['blockageType'])+" "+str(row['severityCategory'])","+str(row['Bus Routes Affected']) +'\n'
                #str(row['blockageType'])+","
                #lineCsv = str(row['id'])+"," +str(row['type'])+","+str(row['blockageType'])+","+str(row['classification'])+","
                #lineCsv+=str(row['road']) +","+str(row['loggedTime'])[0:8]+","+str(row['Bus Routes Affected']) +'\n'
                f.write(lineCsv)
        s3_client.upload_file(tmNew,Bucket=bucket_name,Key=key_list_view)
    #return

#Come back to this part of the code
#1) need to determine the frequency to write out and - not sure how to get out each 15min interval...
#Maybe load up csv and then check the file to see the same time and day - if the both eqal don't write out...

#2) or write out to include the maximum or moving average????

#Important make sure you check and include the number of crashes count!!!
    
#=======================================================================================================
    #write out statistics.  
    #HERE -- Need to figure out how ofted this is called.
    #------- need to move this to a function
    #function to split file into intervals. currently set to 15min intervals
    

    #checking new data for only in progress, convert id type to integer, reindex 
    # new_data = df.loc[df['status'] == 'In Progress']
    # crash_data = new_data.loc[new_data['type'] == 'Crash']
    # total_inc = len(new_data) #Total in progress 
    # total_crsh = len(crash_data) #Total crash
    
    # GET DATE AND WRITE OUT...
    print total_inc
    print total_crsh
    
    #get date as name 
    dayOfWeek = dt.today().strftime('%A')
    print today
    
    def round_time(time, round_to):
        """roundTo is the number of minutes to round to"""
        rounded = time + datetime.timedelta(minutes=round_to/2.)
        rounded -= datetime.timedelta(minutes=rounded.minute % round_to,
                                      seconds=rounded.second,
                                      microseconds=rounded.microsecond)
        return rounded
    nameCsvStat = datetime.datetime.strftime(round_time(dt,15),"%H%M") #format

    #total_inc
    key="stat/"+nameCsvStat+".csv"
    #key=nameCsvStat+".csv"
    print "write out stats..."
    #print key
    
    #check file exist
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    objs = list(bucket.objects.filter(Prefix=key))  
    
    if objs == []:
        print "no file - Print out"
        tmNew=r"/tmp/tmp3.csv" 
        with open(tmNew,"w") as f: 
            f.write('Date,Weekday,IncidentCount,CrashCount'+ '\n') #headers 
            lineCsv = today+","+dayOfWeek+","+str(total_inc)+","+str(total_crsh)
            f.write(lineCsv)
        s3_client.upload_file(tmNew,Bucket=bucket_name,Key=key)    
    else:
        print "file exists - Check compare and rewrite"
        #load the whole file and then check  - for updates and rewrie
        response = bucket.Object(key=key).get()
        csvdf = pd.read_csv(io.BytesIO(response['Body'].read()), encoding='utf8')
        
        print csvdf
        df1 = csvdf[csvdf['Date'].astype(str).str.contains(today)]
        print df1
        if csvdf[csvdf['Date'].astype(str).str.contains(today)].empty: #check if its the same day   #chage out the 'not statemtne'  - only write out...
            print "already ran today!!"
            csvdf = csvdf.append({'Date': today, 'Weekday': dayOfWeek, 'IncidentCount': str(total_inc),'CrashCount': str(total_crsh)}, ignore_index=True)     
            tmNew=r"/tmp/tmp3.csv" 
            with open(tmNew,"w") as f: 
                f.write('Date,Weekday,IncidentCount,CrashCount'+ '\n') #headers 
                for index, row in csvdf.iterrows():
                    lineCsv = str(row['Date'])+"," +str(row['Weekday'])+","+str(row['IncidentCount'])+","+str(row['CrashCount'])+'\n'
                    f.write(lineCsv)
            s3_client.upload_file(tmNew,Bucket=bucket_name,Key=key) 
            print csvdf
    #return    
    
#=======================================================================================================
    #write out summarise analytic incidents for dashboard.
    print 'summarised incidens start...'
    
    time_format = '%H.00-%d/%m'
    
    #build a rolling 24 hour template to map summarised data to
    df_24hr_template = pd.DataFrame({
    'loggedTime': [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
    })
    #iterate through dataframe
    for i, row in df_24hr_template.iterrows():
        time_input = dt - timedelta(hours = i)
        df_24hr_template.at[i,'loggedTime'] = time_input.strftime(time_format)
    df_24hr_template=df_24hr_template[::-1]
    
    #group summary
    sum_data = sum_data[['region','loggedTime']]
    sum_data['loggedTime']= sum_data['loggedTime'].astype(object)
    sum_data['loggedTime']= sum_data['loggedTime'].dt.strftime(time_format) #here!@!!
    sum_data = sum_data.dropna(how='any',axis=0)  #removes all none values


    summarise_incidents_timeseries = sum_data.groupby('loggedTime',sort=False).count()
    summarise_incidents_timeseries = summarise_incidents_timeseries[::-1]
    summarise_incidents_timeseries = summarise_incidents_timeseries.reset_index()
    summarise_incidents_timeseries = df_24hr_template.join(summarise_incidents_timeseries.set_index('loggedTime'), on=['loggedTime'])
    summarise_incidents_timeseries['group'] = 'All'

    #southcoast
    sth_data = sum_data[sum_data['region'].str.contains("South Coast")]
    sth_data = sth_data.groupby('loggedTime',sort=False).count()
    sth_data = sth_data[::-1]
    sth_data = sth_data.reset_index()
    sth_data = df_24hr_template.join(sth_data.set_index('loggedTime'), on=['loggedTime'])
    sth_data['group'] = 'Sth Coast'
    summarise_incidents_timeseries = summarise_incidents_timeseries.append(sth_data)

    #northcoast
    nth_data = sum_data[sum_data['region'].str.contains("North Coast")]
    nth_data = nth_data.groupby('loggedTime',sort=False).count()
    nth_data = nth_data[::-1]
    nth_data = nth_data.reset_index()    
    nth_data = df_24hr_template.join(nth_data.set_index('loggedTime'), on=['loggedTime'])
    nth_data['group'] = 'Nth Coast'
    summarise_incidents_timeseries = summarise_incidents_timeseries.append(nth_data)
   
    #metro
    met_data = sum_data[sum_data['region'].str.contains("Metropolitan")]
    met_data = met_data.groupby('loggedTime',sort=False).count()
    met_data = met_data[::-1]
    met_data = met_data.reset_index()   
    met_data = df_24hr_template.join(met_data.set_index('loggedTime'), on=['loggedTime'])
    met_data['group'] = 'Metro'
    summarise_incidents_timeseries = summarise_incidents_timeseries.append(met_data)

    summarise_incidents_timeseries.fillna(0, inplace=True)
    summarise_incidents_timeseries['region']= summarise_incidents_timeseries['region'].astype(int) #keep count as interger
    #print summarise_incidents_timeseries

    #--------------------------------------------------------upload

    session = boto3.Session()
    s3_client = session.client('s3')
    
    #writes out csv live Visualisation (deck.gl) - but only the records with coordinates (able to be displayed)
    tmNew=r"/tmp/tmp.csv"
    with open(tmNew,"w") as f:
        f.write('group,category,measure'+ '\n') #headers
        for index, row in summarise_incidents_timeseries.iterrows():
            lineCsv = str(row['group'])+","+str(row['loggedTime'])[:4]+","+str(row['region']) +","+ '\n'   
            f.write(lineCsv)
    s3_client.upload_file(tmNew,Bucket=bucket_name,Key=key_dashboard)
    
    print 'uploaded - sumarised values'
    
    return
    