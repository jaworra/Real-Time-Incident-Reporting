#    This function takes input from acciddents  (lat long) and relatest to other sources
#    WAZE | BOM |  QPS | HERE etc ..

import json
import time


#for HERE api Global Tokens
app_id = ''
app_code = ''



try:
    from botocore.vendored import requests
except ImportError: #have to get it in AWS Lambda from here instead
    import requests

from random import random, randint
from numpy import mean
from math import sqrt    

from utmconversion import from_latlon    




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




#closes point from a list of points - optimised algorithm
def closes_pt(pt_list_unsorted,p2):
    '''function requires a point lat long and 
    a list of points to compare the closes coordinate'''
    
    Best = [None, None, float("inf")]
    pt_list_sorted = sorted(pt_list_unsorted)
    for p1 in pt_list_sorted:
        dx = p1[0]-p2[0]
        dy = p1[1]-p2[1]
        if dx < Best[2] and dy < Best[2]: #add here a condition of largest value
            dist = sqrt(dx**2 + dy**2)
            if dist < Best[2]:
                Best = p1, p2, dist
    return Best
    
    
    
def current_here_links_flow(incCsv_dict, here_prox, number_of_incidents): #Current related here links 
    '''function that takes coordinates from incidents and id
    returns dictionary links
    
    Input:
    incCsv_dict (Incident dictionary) - {16939653: [153.06776430000002, -27.65454102, 'In Progress', 'Unknown', 'None', '10:27:48 10-01-2020'], 
    here_prox (Incident proximtey in meters) - 100
    number_of_incidents (cut of due to priority and computation complexithy) - 3
    
    Output:
    {'name': u'Beaudesert - Nerang Road', 'avSpeed': 30.66, 'jamF': 1.57294, 'cords': '[[153.33626,-27.98915],[153.33633,-27.98922],[153.33643,-27.9893],[153.3365,-27.98935]]', 'id': [153.333
    
    '''
    
    i=0
    for key, value in incCsv_dict.items():
        i +=1
        #### optimise - crashes 
        ### webpage for top 10 crashes and impacts on the network.        
        if i > number_of_incidents: #limit - network error, investigate multiple API call
            break       

        incidentCord = str(value[1]) + "," + str(value[0])
        incidentId = str(key)
        
        print '---ss'
        print incidentId
        
        here_flow_dict = {'id': None, 'name': None, 'avSpeed': None , 'jamF': None, 'cords': None}
             
        #configure session request API
        starttime = time.time()
        urlsession = requests.session()
        #configure payload
        url = "https://traffic.api.here.com/traffic/6.2/flow.json?app_id=" + app_id + "&app_code=" + app_code
        url +="&prox="+incidentCord+","+str(here_prox)+"&responseattributes=sh,fc"
        
        #send request
        response = requests.get(url, timeout=600)    
        response = response.content
        #clean up
        urlsession.close()
    
        z=1
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
                                    #print(linRd)
                                    flowInfoSpeed = el4['CF'][0].get('SU') #Speed (based on UNITS) not capped by speed limit
                                    flowInfoJam = el4['CF'][0].get('JF') # The number between 0.0 and 10.0 indicating the expected quality of travel. When there is a road closure, the Jam Factor will be 10. As the number approaches 10.0 the quality of travel is getting worse. -1.0 indicates that a Jam Factor could not be calculated
                                    flowInfoCon =  el4['CF'][0].get('CN') #Confidence, an indication of how the speed was determined. -1.0 road closed. 1.0=100% 0.7-100% Historical Usually a value between .7 and 1.0
                                    for el5 in el4['SHP']: #get shape file
                                        cordStr = changeCoordsStr(el5)
                                        here_flow_dict[z] = {'id': incidentId, 'name': linRd, 'avSpeed': flowInfoSpeed , 'jamF': flowInfoJam, 'cords': cordStr}
                                        z+=1
                                        #dfHere.loc[len(dfHere)] = [incidentId, linRd, flowInfoSpeed,flowInfoJam,cordStr]

            except Exception as ex:
                print(str(response))
                raise ex  

    #print '6666'
    #print here_flow_dict
    print '---end of program ---'
    x=z-1
    # print str(x)
    print str(x)
    print here_flow_dict[x] 
    return here_flow_dict
    

def current_waze(): #Current waze incidents

    '''
    Check WAZE for incidents and return a list of current alerts 
    3 types of events available in Event Feed
    Alerts, Irregularities, Jams

    Jams/Congestion (passively collected for users)
    Alerts (actively submitted by users)
    Irregularities - speeds on slower roads

    Alerts sample
    {'confidence': 0,
    'country': 'AS',
    'location': {'x': 153.097267, 'y': -26.675561},
    'magvar': 144,
    'nThumbsUp': 0,
    'pubMillis': 1576639862000,
    'reliability': 6,
    'reportRating': 1,
    'roadType': 3,
    'street': 'SR70 - Sunshine Mtwy',
    'subtype': 'HAZARD_ON_SHOULDER_CAR_STOPPED',
    'type': 'WEATHERHAZARD',
    'uuid': 'e9688542-2693-3a08-8199-78f4f8ed0251'}
    '''

    #Waze up session and payload to waze
    url = "https://world-georss.waze.com/rtserver/web/TGeoRSS?tk=ccp_partner&ccp_partner_name=Queensland&format=JSON&types=traffic,alerts,irregularities&polygon=138.010254,-16.261152;138.032227,-26.041052;140.976563,-26.021308;140.998535,-29.040863;154.291992,-29.194429;154.006348,-22.962503;142.492676,-9.626815;140.822754,-10.924000;138.010254,-16.261152;138.010254,-16.261152"
    session = requests.Session()
    response = session.get(url)
    waze_json = response.json()

    
    #create two waze list - 1) UTM coordinates 2) lat long coordinates with attributes 
    #Only parse south east corridor
    waze_SE_lat_long_with_attributes = []
    waze_SE_utm = []
    for alerts in waze_json['alerts']:
        locations = alerts['location']
        lon_temp = locations['x'] 
        lat_temp  = locations['y']
        
        if -28.2 <= lat_temp < -26 and 152 <= lon_temp < 154:  #South east corridor (north)-26.179091, 152.652139 (south) -28.192732, 153.527047
            waze_SE_lat_long_with_attributes.append((alerts['type'],lat_temp,lon_temp))

            #converstion to UTM
            limiteasting, limitnorthing, _ , _ = from_latlon(latitude=lat_temp, longitude=lon_temp, force_zone_number=56)
            waze_SE_utm.append((limitnorthing , limiteasting))

    return waze_SE_utm,waze_SE_lat_long_with_attributes
    

def current_holidays(date): #Current waze incidents
    '''Searches if date is a holiday in queensland, using CKAN
    https://data.gov.au/data/dataset/australian-holidays-machine-readable-dataset
    returns true or fasle and name of holiday
    '''

    
    year = date[:4]
    if year == '2017':
        resource_id = 'a24ecaf2-044a-4e66-989c-eacc81ded62f'  
    elif year == '2018':
        resource_id = '253d63c0-af1f-4f4c-b8d5-eb9d9b1d46ab'  
    elif year == '2019':
        resource_id = 'bda4d4f2-7fde-4bfc-8a23-a6eefc8cef80'  
    elif year == '2020':
        resource_id = 'c4163dc4-4f5a-4cae-b787-43ef0fcf8d8b'
    else:
        return "N/A - No Service" , ""
    
    #config payload
    url = 'https://data.gov.au/data/api/3/action/datastore_search?'
    url +='resource_id='+resource_id
    url += '&limit=5&q={"Date":"'+date+'", "Jurisdiction":"qld"}' 
    #url += '&fields="Holiday Name"' #restrict only field names
    
    '''
    Sample response
      {
        "_id": 41,
        "Raw Date": "1555596000",
        "Date": "20190419",
        "Holiday Name": "Good Friday",
        "Information": "Easter is celebrated with Good Friday and Easter Monday creating a 4 day long weekend.",
        "More Information": "http://www.justice.qld.gov.au/fair-and-safe-work/industrial-relations/public-holidays/dates",
        "Jurisdiction": "qld",
        "rank": 0.0573088
      },
    '''

    #Call request
    session = requests.Session()
    response = session.get(url)
    holiday_json = response.json()
    
    #Process Json
    result = holiday_json.get("result")
    result_total = result.get("total")
    
    if result_total > 0:
        result_records = result.get("records")
        result_holidays = result_records[0]
        holiday = result_holidays.get("Holiday Name")
    else:
        return "False",""
    
    return "True" , holiday 
        

def current_roadtek_vehicles():
    
    '''
    Check qbit road teck traffic for list of incidents and return a list of current alerts 
    
    id  = the unique id of the device in Qbit
    trid = the value the device is recognized from, usually an IMEI, ESN or BlackBerry PIN
    nm = device name
    ph = device phone number
    uid = the ID of a Qbit user assigned to this device
    ic = the icon filename
    st = a user-enterable status string (currently unused)
    fd = current geofence size in feet (for web fencing)
    tzos = time zone offset in minutes
    odt = current odometer time in hours (2 decimal places)
    odd = current odometer distance in units specified by oddu (1 decimal place)
    oddu = units for odometer distance
    prid = unique id for the type of device (product ID)
    prnm = name for the type of device (product name)
    perm = some permission data for the device, disregard

    1. Location where user was stopped:
    
    tm = the epoch time (in local device time, not UTC) that the device entered that location. This is the number of seconds since midnight Jan 1, 1970.
    utc = the epoch time (in UTC) that the device entered that location. This is the number of seconds since midnight Jan 1, 1970.
    stp="1" signifies that this is a "stop" location rather than a "travel" location, in which case the attribute is omitted.
    tmout = the local epoch time that the device departed the stop (the time of the first location after the stop)
    tmspan = the number of minutes stopped at that location
    lat = the latitude of the location
    lng = the longitude of the location
    spd = the speed of the device at that location (only included if speed > 0) by default in km/h, but should be in whatever units that device is set to use
    hdg = the compass heading in degrees at this location
    sats = the number of satellites in view
    hdop = the horizontal dilution of precision of the location
    
    Child tags:
    The <gc1> tag indicates the smallest known geocode (landmark) that this location is inside. The tag includes:
    lat = the latitude of the centroid of the geocode
    lng = the longitude of the centroid of the geocode
    nm = the name of the geocode
    gcID = the GUID ID of the geocode
    grpID = the GUID groupID of the geocode. Only included if this geocode has a group specified.
     
    The <gc2> tag indicates the closest geocode that this location is outside of.
    lat = the latitude of the centroid of the geocode
    lng = the longitude of the centroid of the geocode
    nm = the name of the geocode
    gcID = the GUID ID of the geocode
    grpID = the GUID groupID of the geocode. Only included if this geocode has a group specified.
    dst = the distance from the this device location to the centroid of this geocode in km
    hdg = the compass heading in degrees from the geocode to the current device location
  
    
    2. Location during travel:

    tm = the epoch time (in local device time, not UTC) that the device entered that location
    lat = the latitude of the location
    lng = the longitude of the location
    spd = the speed of the device at that location
    hdg = the compass heading at this location in degrees
    sats = the number of satellites in view
    hdop = the horizontal dilution of precision of the location

    3. Location with exception: 
    ex = exception:
    et = exception type (see below)
    nm = exception name
    exdsc = exception description
    exid = exception GUID ID

    '''
    
    
    #Waze up session and payload to waze
    url = "https://ec2.qbitmobile.com/services/getDevices.aspx?t=612613ac-e4ae-4679-a901-10db138c5149&format=json"
    session = requests.Session()
    response = session.get(url)
    traffic_json = response.json()
    
    

    print traffic_json
    return


def current_weather(latitude, longitude):
    
    #weather session and payload
    #api_key = "d1741b0c4ca70aeb629424a1ddcf28a1"
    api_key = "a702fc7ad1caec9619d26cda222bb5ef"
    getParameters = {'appid':api_key}

    
    openweather_url = "https://api.openweathermap.org/data/2.5/weather"
    openweather_url += "?lat="+latitude+"&lon="+longitude
    openweather_url += "&units=metric"
    session =requests.Session()
    weather_response = session.get(openweather_url,params=getParameters)
    
    '''
    typecial Json return
    {'coord': {'lon': 153.02, 'lat': -27.47},
    'weather': [{'id': 521,
    'main': 'Rain',   #return this value
    'description': 'shower rain',
    'icon': '09n'}],
    'base': 'stations',
    'main': {'temp': 294.59, #return this value
    'pressure': 1028,
    'humidity': 82,
    'temp_min': 292.04,
    'temp_max': 297.04},
    'visibility': 10000,
    'wind': {'speed': 5.1, 'deg': 100},
    'clouds': {'all': 75},
    'dt': 1569887073,
    'sys': {'type': 1,
    'id': 9485,
    'message': 0.0066,
    'country': 'AU',
    'sunrise': 1569871679,
    'sunset': 1569916055},
    'timezone': 36000,
    'id': 2174003,
    'name': 'Brisbane',
    'cod': 200}
    '''
    

    #response - check 
    if weather_response.status_code == 200:
        position_weather = weather_response.json()
        temp = position_weather['main']['temp'] # in kelvin
        outlook = position_weather['weather'][0]['main']
    else:
        return 'N/A','N/A'
    
    return temp, outlook


    
