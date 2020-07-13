

#Purpose: Dumps all Lambdas out into a s3 bucket periodically, for backup purposes


#Lambda Timeout: 3 minutes

#instance type: 128MB is fine

#set environment variables:
# Desired_Interval_Seconds 6000  (for every 100 minutes)

outbucketname=""
proxies= {}	



import datetime
import time
import json
import io

import zipfile

import boto3

try:
	import requests
except ImportError:
	from botocore.vendored import requests

	
client = boto3.client('lambda')
s3 = boto3.resource('s3')

#from catchExceptions import catchExceptions #.py, for:	
#@catchExceptions(returnErrStringOfException=False,returnValueOnException=None,printToConsole=False,toFilePath=None,toHandlerExpectingAString=None): #decorator to catch all exceptions and optionally print them to console and/or a file and/or pass the message to a handler
#Decorator for functions, to catch their exceptions and redirect them to console, and/or a handler, and/or a (rotating) log file; and/or to return a specific value.  All parameters are optional
#use as eg: 
#def handler(txt):
# pass #txt is a string which describes the error
#
#@catchExceptions(toHandlerExpectingAString=handler,toFilePath='/tmp/mylog.txt', printToConsole=False,returnValueOnException=42)
#def f(x):
# return x/0  #due to decorator, if we are passed 0 as a parameter, we will return 42 as a result and log the exception to a file as well
#
#When exceptions are redirected, the FULL call stack as well as local variable values are included



def unixtime_subsecs(): #return unixtime (time-zone independant time) to sub-second precision
	#time.time()  #may just perhaps not always return in UTC epoch time??
	dts = datetime.datetime.utcnow()
	return time.mktime(dts.timetuple()) + dts.microsecond/1e6


def unixtime_secs(): #return unixtime rounded to nearest second
	return int(round(unixtime_subsecs()))
	
	
#Logging assistance
def log(txt):
	print (datetime.datetime.utcnow()+datetime.timedelta(hours=10)).strftime("%Y-%m-%d %H:%M:%S")+" (UTC+10)"+" "+str(txt)


##################################################################################################################################	
#Code run on instance start follows. All functions/etc it uses needs to be created above this line
	
log("Instance starting") #log something asap, just in case

def errhandler(txt):
	raise Exception(txt) #throw it, now its been augmented with full stack trace and local variable values


#Entry point
#@catchExceptions(printToConsole=True, toHandlerExpectingAString=errhandler)
def lambda_handler(event=None, context=None):   #return any warnings as a string; throw an exception if we couldn't complete so our caller can catch it and tell others about it
	#If we can retry something in here if we detect a transient error, then do it
	
	log("Handler called at unixtime "+str(unixtime_subsecs()))

	#paranoia, shouldn't happen
	if event.get('unixtime_subsecs') is not None and abs(event['unixtime_subsecs']-unixtime_subsecs())>10.0:  
		return

	dt=datetime.datetime.utcnow() + datetime.timedelta(hours=10)
	dts=datetime.datetime.strftime(dt, "%Y%m%d_%H%M%S")
	key=datetime.datetime.strftime(dt, "year_utcplus10=%Y/month_utcplus10=%m/day_utcplus10=%d/yyyymmdd_hhmmss_utcplus10_%Y%m%d_%H%M%S_lambda_backup.zip")	
	
	#zip file to dump everything into
	megazippath="/tmp/alllambdas.zip"
	zf = zipfile.ZipFile(megazippath, mode="w")
	
	paginator = client.get_paginator('list_functions')	
	response_iterator = paginator.paginate(
	    #MasterRegion='string',
	    #FunctionVersion='ALL',
	    PaginationConfig={
	        'MaxItems': 999999,
	        'PageSize': 10 #,
	        #'StartingToken': 'string'
		    }
		)
	print(response_iterator)
	for page in response_iterator:
		for rec in page['Functions']:
		#for rec in client.list_functions()['Functions']: #NO! returns only 50, then a token to get more. Use a paginator..
			lambdaname=rec['FunctionName']
			log("Processing "+lambdaname)
			
				
			config = client.get_function(FunctionName=lambdaname)
			
			#drop sensitive environment variable values 
			# to ensure we don't expose passwords (environment variables are stored encrypted in AWS)
			if config['Configuration'].get('Environment') is not None:
				if config['Configuration']['Environment'].get('Variables') is not None:
					for k,v in config['Configuration']['Environment']['Variables'].items(): #environment variable values are strings
						#see if is numeric
						tmp=None
						try:
							tmp=float(v)
						except Exception:
							pass
						if tmp is None or ('secret' in k.lower()) or ('password' in k.lower())  or ('token' in k.lower()): #assume numbers aren't sensitive (unless called 'password' or 'token'), everything else is
							config['Configuration']['Environment']['Variables'][k]="<<HIDDEN_DURING_BACKUP>>"
			#end of 'if config..' environment-variable password eliding check
			
			#print repr(config)
			
			#download the zip of its sourcecode
			url=config['Code']['Location']
			response = requests.get(url,proxies=proxies,timeout=30) #verify=False to ignore SSL certificate errors
			if response.status_code!=200:
				raise Exception("Request to AWS for lambda code for '"+lambdaname+"' returned status code "+str(response.status_code)+", expected 200. Url "+url);
	
	
			#write to temp files then include in the big zip we're building
			with io.open("/tmp/"+lambdaname+".zip","wb") as f:
				f.write(response.content)
			zf.write("/tmp/"+lambdaname+".zip", lambdaname+".zip", compress_type=zipfile.ZIP_DEFLATED)
			
			with io.open("/tmp/"+lambdaname+".json","wb") as f:
				f.write(json.dumps(config))
			zf.write("/tmp/"+lambdaname+".json", lambdaname+".json", compress_type=zipfile.ZIP_DEFLATED)
		
		#end of 'for rec'
	#end of 'for page'
	
	zf.close()
	s3.Bucket(outbucketname).upload_file(megazippath,key)
	

	log("All done")
	
	return "" #no warnings to return