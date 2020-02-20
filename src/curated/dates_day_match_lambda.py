import csv#take this out!!!
import math


# Get all CSV files in the bucket
def get_csv_files(client, bucket):
    csv_files = []
    content = client.list_objects(Bucket=bucket).get('Contents')
    for obj in content:
        key = obj.get('Key')
        if '.csv' in key:
            csv_files.append(key)
    return csv_files
    
   
    
def check_file_fordates(client, s3_key, bucket):
    '''
    check each file for dates
    '''
    print "updating file: "+s3_key

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketname_routes)
    obj = bucket.Object(s3_key)
    response = obj.get()
    lines = response[u'Body'].read().split()
    
    #Send out to S3
    with open("/tmp/temp.csv", 'w') as h:
        h.write('Date,Weekday,IncidentCount,CrashCount'+ '\n')

        for row in csv.DictReader(lines):

            date = str(row['Date'])
            weekday = str(row['Weekday'])
            incident_count = row['IncidentCount']
            crash_count = row['CrashCount']
            
            date_input = datetime.date(int(date[0:4]),int(date[4:6]),int(date[6:8]))
            calculated_weekday = date_input.strftime('%A')
            
            if weekday != str(calculated_weekday): #if discrepncies date doesn't match
                weekday = calculated_weekday
                
            #convert types 
            if crash_count == "nan":    
                crash_count = 0
            elif type(crash_count) is str:
                crash_count = int(round(float(crash_count)))
    
                
            if incident_count == "nan":
                incident_count = 0
            elif type(incident_count) is str:
                incident_count = int(round(float(incident_count)))
                
            #write out
            lineCsv = str(date)+","+str(weekday)+","+str(incident_count)+","+str(crash_count)+'\n'
            h.write(lineCsv)
            #h.write("%s,%s,%s,%s\n" % (date,weekday,str(incident_count),str(crash_count)))
    
 
    outbucket=s3.Bucket(bucketname_routes)
    outbucket.upload_file("/tmp/temp.csv", s3_key[0:9]+'_new.csv') 
    print 'writtern to... ' +  s3_key[0:9]+'_new.csv'
    return
    

def qa_checking_dates():
    '''
    some errors with assigning day from date  13/02/2020 should be thursday instead assigned friday
    '''
    #list of s3 in bucke with key
    session = boto3.Session()
    s3_client = session.client('s3')
    
    for key in get_csv_files(s3_client, bucketname_routes):
        key_string=str(key)
        if key_string[0:5] == 'stat/' and key_string[0:11]  != 'stat/bytime' and key_string[0:8]  != 'stat/qry' and \
        key_string != 'stat/rolling_4day_window.csv' and key_string != 'stat/typical_friday.csv' and key_string != 'stat/typical_monday.csv' and \
        key_string != 'stat/typical_saturday.csv' and key_string != 'stat/typical_sunday.csv' and key_string != 'stat/typical_thursday.csv' and \
        key_string != 'stat/typical_tuesday.csv' and key_string != 'stat/typical_wednesday.csv' and key_string[9:17]  != '_new.csv':
            #update specific csv file
            check_file_fordates(s3_client,key_string,bucketname_routes)
            #return
    return   



def lambda_handler(event, context):
    
    try:
        qa_checking_dates()
    except Exception, err:
        print err
        return
    
    print 'SUCCESSFUL COMPLETED - lamabda'
    
    return 