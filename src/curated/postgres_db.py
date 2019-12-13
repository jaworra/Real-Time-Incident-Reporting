#Copied from master 4Apr19


#Exports:
# def open_database_connection_with_autocommit_off() #returns a DB-API connection you can pass to these routines or call .cursor() on
# def execute_sql(sql,conn) #returns a cursor, will rollback the transaction on an exception. call conn.commit() if want to commit results!
# def rollback()
# def commit()
# def fetchone_asdict(cursor_or_sql_or_listofsql,conn_if_supplied_sql_or_listofsql=None): #may commit any existing transaction
# def fetchall_aslistofdict(cursor_or_sql_or_listofsql,conn_if_supplied_sql_or_listofsql=None):#may commit any existing transaction
#Example usage:
# e.g. fetchone_asdict(cursor)
# e.g. fetchone_asdict("select * from mytable",conn)
#Remember that autocommit is off, so you have to explicitly call conn.commit() to save your changes!!


import pg8000
import socket #for connection pre-test
import time

def _setlocktimeout(conn):  #this is run at the start of each of our transactions
    #NOTE: we run this initially, and also after rollback and commit. This however starts a new transaction, so 
    # to run something outside of a transaction, one needs to do:
    # conn.autocommit=False
    # conn.commit() #NOT commit(conn), as that calls us here
    
    #don't wait forever for locks we are waiting on to be released, instead throw an error
    execute_sql("SET lock_timeout TO "+str(conn._maxtimetowaitforlocks),conn,silent=True) 
    
    #don't hold locks forever if we crash/exit in a transaction without doing a commit() or rollback()
    #tell the database to kill our transaction (actually: our entire connection!) if it takes longer than it should (plus a little bit: 2 seconds)
    killtime=conn._killtransactionsatthistime #get the time.time() value (seconds past epoch) at which our lambda will timeout
    timetogountilwedie=killtime - time.time()
    execute_sql("SET idle_in_transaction_session_timeout to "+str(long((2.0+timetogountilwedie)*1000.0)),conn, silent=True) #takes number of millisecs
    
    
    


#def set_max_transaction_duration_seconds(secs, conn): #just for this transaction (overriding the default done in open_database..), cap the time until a commit or rollback needs to happen. If this time is exceeded, the database connection will be CLOSED
#    execute_sql("SET idle_in_transaction_session_timeout to "+str(secs*1000.0),conn) #takes number of millisecs


def open_database_connection_with_autocommit_off(db_name,db_user,db_password,db_host,db_port=5432, max_millisecs_connection_will_be_open_for=None, max_time_to_wait_for_locks_ms=2*60*1000): #returns a DB-API connection you can pass to these routines or call .cursor() on
 
 #re: max_millisecs_connection_will_be_open_for
 #A good value for max_millisecs_connection_will_be_open_for is context.get_remaining_time_in_millis(), but use a smaller number if possible/sensible (e.g. if the lambda has a 15min timeout but the postgres stuff should complete quickly)
 #If a transaction is still in progress at this time (isn't rolledback or committed), the whole database connection will be closed and the transaction auto-rolled-back by the database EVEN if the lambda is no longer executing
 #The reason we care is that if a lambda exits/dies with a transaction which is in progress, any locks it holds will 'never' be released in the postgres database (hence affecting other users, or other executions of this same lambda) :( :(,
 # necessitating a call to request_cancellation_of_deadlocked_or_otherwise_longrunning_tasks() or more-likely request_termination_of_deadlocked_or_otherwise_longrunning_tasks()
 # to clean them up and release those locks
 #Can be overriden in any given transaction if needed via a execute_sql('set idle_in_transaction_session_timeout to ..') call

 #re: max_time_to_wait_for_locks_ms
 #"Abort any statement that waits longer than the specified number of milliseconds while attempting to acquire a lock on a table, index, row, or other database object. The time limit applies separately to each lock acquisition attempt."
 #Default value of 2 minutes is applied here, but can be overriden or set with a execute_sql('SET lock_timeout TO ') within any given transaction
 
 
 #First, ensure we can even reach the port the database is listening on before we try to connect
 #can't set a pg8000.connect timeout in python v2.7!! "For communicating with the server, pg8000 uses a file-like object using socket.makefile() but you can't use this if the underlying socket has a timeout."
 _ensure_remote_port_is_open(db_host,db_port) #won't work behind a proxy!!!
 print "Connecting to Postgres cluster "+db_host +" on port "+str(db_port) +", database "+db_name+", as user "+db_user
 conn = pg8000.connect(ssl=True,database=db_name,user=db_user, password=db_password, host=db_host, port=db_port) #cannot specify a timeout :(
 print "Connected ok"
 
 #conn.autocommit(False) #for MySQL
 conn.autocommit=False  #for PG8000. Just to be sure, ensure autocommit is off so we can do db.commit() and db.rollback()- there's no call to start a transaction, that's implicit
 #autocommit off is a good choice to run with, as otherwise e.g. each insert will have a commit done after it which is really slow compared to doing all inserts in a transction then commit'ing the transaction
 
 #record in the connection object how long we should wait for locks we need
 setattr(conn,'_maxtimetowaitforlocks',max_time_to_wait_for_locks_ms)
 
 #record in the connection object the value from time.time() at which we want the database to auto-rollback any outstanding transactions of ours (it'll also kill our connection but we won't get an exception) if they haven't completed by then, even if we are no longer running
 if max_millisecs_connection_will_be_open_for is not None:
     if max_millisecs_connection_will_be_open_for<0 or max_millisecs_connection_will_be_open_for>15*60*1000 or max_millisecs_connection_will_be_open_for<2000:
          raise Exception("Invalid value for 'max_millisecs_connection_will_be_open_for', needs to be at least 2000ms and not more than 15*60*1000 (15 minutes)")
     else:
         pass #good to go!
 else:
     print("WARNING- a value has not passed in for 'max_millisecs_connection_will_be_open_for' so a long failsafe time of 15min is being applied for all transactions, ensuring any transactions that aren't rolled-back or committed before this lambda exits or dies get their locks released eventually")
     max_millisecs_connection_will_be_open_for = 15*60*1000 #15 minutes, which is the longest a lambda can run for
 killtime=time.time() + (max_millisecs_connection_will_be_open_for/1000.0)   #Value of time.time() (whcih returns seconds) to kill any open transaction at if it is still running
 setattr(conn,'_killtransactionsatthistime', killtime) #add two seconds, just in case
 
 _setlocktimeout(conn)
 
 return conn


def _ensure_remote_port_is_open(host,port):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client_socket.connect((host,port))
        return 
    except Exception as err:
        raise Exception(host+":"+str(port)+" doesn't seem to be reachable")
    finally:
        client_socket.close()


def execute_sql_outsideoftransaction_afterdoingacommit(sql,conn,silent=False): #use this for e.g. "vacuum analyze >yourtablename<", as our setup otherwise is that everything is run in a transaction
    tmp_ac=conn.autocommit
    conn.commit() #have to do this rather than commit(conn), as our postgres code in 'commit()' does a 'SET' command after the commit and hence starts a new transaction meaning autocommit=True below would be ineffective
    conn.autocommit=True #needed for vacuum, it can't run in a transaction.  NB: execute_sql("SET AUTOCOMMIT TO ON",conn) doesn't work, execute_sql(""\set AUTOCOMMIT ON") might though..
    try:
        execute_sql(sql,conn,silent=silent) 
    finally:
        conn.autocommit=tmp_ac #back to what it was
        commit(conn) #set lock timeout etc; i.e. reset to where we'd be if we didn't do this stuff at all



def execute_insert(tablename,listofdic,conn,batchsize=10000,silent=False): #I'm not sure what PG8000s limit is when it comes to max batchsize..
    sql_needing_params=None
    params=[]
    for cv in listofdic:
        if sql_needing_params is None:
            sql_needing_params="INSERT INTO "+tablename+" ("+",".join([k for k,v in cv.items()])+") VALUES ("+",".join(['%s' for k,v in cv.items()])+")"
        params.append( tuple(v for k,v in cv.items()) )
        if len(params)>=batchsize:
            execute_sql_many(sql_needing_params,params,conn,silent=silent)
            params=[]
    if sql_needing_params and len(params)>0: 
        execute_sql_many(sql_needing_params,params,conn,silent=silent)
        


def execute_upsert(tablename,listofdic,listofpkfieldnames,conn,batchsize=10000,silent=False): #insert or if already present, update. I'm not sure what PG8000s limit is when it comes to max batchsize..
    params=[]
    sql_needing_params=None
    for cv in listofdic:
        if sql_needing_params is None:
            sql_needing_params="INSERT INTO "+tablename+" ("+",".join([k for k,v in cv.items()])+") VALUES ("+",".join(['%s' for k,v in cv.items()])+") " + \
                "ON CONFLICT ("+",".join(listofpkfieldnames)+") DO UPDATE SET "+",".join([k+"=EXCLUDED."+k for k,v in cv.items() if not k in listofpkfieldnames])
        params.append( tuple(v for k,v in cv.items()) )
        if len(params)>=batchsize:
            execute_sql_many(sql_needing_params,params,conn,silent=silent)
            params=[]
    if sql_needing_params and len(params)>0: 
        execute_sql_many(sql_needing_params,params,conn,silent=silent)
        

def execute_sql_many(sql_with_questionmarks,listoftuplesofvalues,conn,silent=False): #will rollback the transaction on an exception. call conn.commit() if want to commit results!
#use this rather than cursor.execute(), as it ensures we do a 'rollback' after any failed execution; if we don't postgresql is weird in that future executions will abort! (until a rollback is done)
    #print sql_with_questionmarks
    #print "params: "+repr(listoftuplesofvalues)[:2000]+'..'
    cursor=conn.cursor()	
    
    try:
        
        cursor.executemany(sql_with_questionmarks,listoftuplesofvalues)
        if not silent:
            print("Executed sql on "+str(len(listoftuplesofvalues))+" tuples: "+sql_with_questionmarks.replace('\n',' '))
    
        
    except Exception as ex:
        if 'unpack_from requires a buffer of at least 5 bytes' in str(ex): #this is the error PG8000 will give us if the database has closed our connection or similar
            ex=Exception('Connection to database has been closed')
            
        if not silent:
            print("Failed to execute sql on "+str(len(listoftuplesofvalues))+" tuples: "+sql_with_questionmarks.replace('\n',' '))
        
        #rollback immediately, to release any locks
        try:
            conn.rollback() 
        except Exception as err:
            if not silent:
                print "On resultant rollback we asked for, had another exception: "+str(err)
        
        raise ex

    finally:
        try:
            cursor.close()
        except:
            pass
        

def execute_sql(sql,conn, silent=False, return_cursor=False): #will rollback the transaction on an exception. call conn.commit() if want to commit results!
#use this rather than cursor.execute(), as it ensures we do a 'rollback' after any failed execution; if we don't postgresql is weird in that future executions will abort! (until a rollback is done)

    cursor=conn.cursor()	
    
    try:
        if not silent:
            print("About to execute sql: "+sql.replace('\n',' '))
        cursor.execute(sql)
        if return_cursor:
            return cursor
        
    except Exception as ex:
        if 'unpack_from requires a buffer of at least 5 bytes' in str(ex): #this is the error PG8000 will give us if the database has closed our connection or similar
            ex=Exception('Connection to database has been closed')

        try:
            cursor.close() #just in case, do first before rollback
        except:
            pass
        if not silent:
            print("Failed to execute sql (exception: "+str(ex)+"): "+sql.replace('\n',' '))
        
        try: #rollback immediately, to release any locks
            conn.rollback() 
        except Exception as err:
            if not silent:
                print "On resultant rollback we asked for, had another exception: "+str(err)

        raise ex
        #if we don't roll-back, postgres will throw a "current transaction is aborted, commands ignored until end of transaction block"
        # when we next try to execute something!
        #"Developers either love or hate this PostgreSQL, or Postgres, error.  
        # For developers new to Postgres or expanding Postgres support to their applications, 
        # the perspective is a little different since they find out that Postgres handles 
        # transactions differently than common databases such as Oracle, SQL Server, or MySQL."
    
    finally:
        if not return_cursor:
            try:
                cursor.close()
            except:
                pass


def rollback(conn):
    conn.rollback
    _setlocktimeout(conn)
    
def commit(conn):
    try:
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except:
            pass

        raise
    finally:
        _setlocktimeout(conn)
        

#Example usage:
# e.g. fetchone_asdict(cursor)
# e.g. fetchall_aslistofdict("select * from mytable",conn)
def fetchone_asdict(sql,conn, silent=False): 
    #convert to cursor if required
    
    cursor=execute_sql(sql,conn,silent=silent, return_cursor=True)
    try:
        cols = [a[0] for a in cursor.description] #get just the field names
        row=cursor.fetchone()
        if row is None:
            res= None
        else:
            res={a:b for a,b in zip(cols, row)}
    
        return res
        
    finally:
        try:
            cursor.close()
        except:
            pass
    

"""
def fetchall_aslistofdict(cursor_or_sql_or_listofsql,conn_if_supplied_sql_or_listofsql=None):
    #convert to cursor if required
    closecur=False
    if isinstance(cursor_or_sql_or_listofsql,list) or isinstance(cursor_or_sql_or_listofsql,basestring):
        if conn_if_supplied_sql_or_listofsql is None:
            raise Exception("Programming error- conn_if_supplied_sql_or_listofsql is None!")
        cursor=execute_sql(cursor_or_sql_or_listofsql,conn_if_supplied_sql_or_listofsql)
        closecur=True
    else: #assume is cursor
        cursor=cursor_or_sql_or_listofsql
    cols = [a[0] for a in cursor.description] #get just the field names
    result=[]
    for row in list(cursor): #cursor.fetchall(): #for some dbs, fetchall() will return an empty tuple rather than an empty list, while list() won't
        result.append({a:b for a,b in zip(cols, row)})
    if closecur:
        cursor.close()
    return result
"""

def fetchall_aslistofdict(sql,conn,silent=False):
    return list(fetchall_asyielderofdict(sql,conn,silent=silent))
    
def fetchall_asyielderofdict(sql,conn,silent=False):
    #convert to cursor if required
    cursor=execute_sql(sql,conn,silent=silent,return_cursor=True)
    try:        
        cols = [a[0] for a in cursor.description] #get just the field names
        for row in cursor: #cursor.fetchall(): #for some dbs, fetchall() will return an empty tuple rather than an empty list, while list() won't
            yield {a:b for a,b in zip(cols, row)}
    finally:
        try:
            cursor.close()
        except:
            pass
    

def table_or_view_exists(tn,conn,silent=False):
    if conn is None:
        raise Exception("Programming error- conn is None!")
    if not silent:
        print "Checking whether table "+tn+" exists in database"
    if fetchone_asdict("SELECT to_regclass('"+tn+"');",conn,silent=True)['to_regclass'] is None:
        return False
    else:
        return True

def request_cancellation_of_deadlocked_or_otherwise_longrunning_tasks(conn): #use this to recover from deadlocks
    for row in fetchall_asyielderofdict("""
        SELECT
          pid,
          now() - pg_stat_activity.query_start AS duration,
          left(query,70),
          state
        FROM pg_stat_activity
        WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes' and state<>'idle';
        """,conn):

            print("Found long-running task: "+ str(row))
            for row2 in fetchall_asyielderofdict("SELECT pg_cancel_backend("+str(row['pid'])+");",conn):
                print("Result from requesting cancellation: "+str(row2))
    

def request_termination_of_deadlocked_or_otherwise_longrunning_tasks(conn): #try the above first
    for row in fetchall_asyielderofdict("""
        SELECT
          pid,
          now() - pg_stat_activity.query_start AS duration,
          left(query,70),
          state
        FROM pg_stat_activity
        WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes' and state<>'idle';
        """,conn):

            print("Found long-running task: "+ str(row))
            for row2 in fetchall_asyielderofdict("SELECT pg_terminate_backend("+str(row['pid'])+");",conn):
                print("Result from requesting termination: "+str(row2))
 

#Other useful examples of stuff follows:
    
#get UTC time
#cursor.execute(""" select 1 as "one",(current_timestamp at time zone 'UTC') as "utc_datetime" """)

#Stored procedure stuff	 
#
# cursor.execute(""" select 1 as "one",(current_timestamp at time zone 'UTC') as "utc_datetime" """)
# cursor.fetchall() #pg8000 casts as a list, second element being a datetime object, first element being 1
#
#this works for posgres, mysql etc have differing syntax
# create or replace function zzz(a int) returns int as $$ begin return a+4; end; $$ language plpgsql;
#ref: https://www.postgresql.org/docs/9.6/static/sql-createfunction.html
#ref: https://www.postgresql.org/docs/9.6/static/plpgsql-overview.html
#
#A function which returns a table consisting of 1 and the current UTC date/time
#def OLD_TEST_recreate_storedfunc_gentable_utcnow(conn):
#  try:
#   execute_as_transaction(conn,"drop function gentable_utcnow()")     
#  except Exception:
#   pass
#  #
#  #if exists but new return type is different from existing, 'create or replace'  won't work and will throw an exception. Hence we try to drop first
#  execute_as_transaction(conn,"""create function gentable_utcnow() returns TABLE(one int, utcnow timestamp) as $$ begin return query select 1 as "one",(current_timestamp at time zone 'UTC'); end; $$ language plpgsql""")


#Conversion from lat-long to MGA zones (55 will do all of Qld); but you can use built-in POSTGIS functions to do this instead..
"""
####################################
#UTM<->LATLONG conversion code

#we want to define function 'from_latlon', but there's some other stuff it needs. Form a closure and get it 
def return_from_latlon(): #returns (at the very end!) the 'from_latlon' funtion defined in this block. We follow this with 'from_latlon=return_from_latlon()'
    #we do it this way to avoid having to have a separate file
    #from https://github.com/Turbo87/utm/blob/master/utm/conversion.py  (MIT license)
    
    #CK modified
    #from utm.error import OutOfRangeError
    class OutOfRangeError(Exception):
        pass
    #end of CK modified
    
    
    # For most use cases in this module, numpy is indistinguishable
    # from math, except it also works on numpy arrays
    try:
        import numpy as mathlib
        use_numpy = True
    except ImportError:
        import math as mathlib
        use_numpy = False
    
    __all__ = ['to_latlon', 'from_latlon']
    
    K0 = 0.9996
    
    E = 0.00669438
    E2 = E * E
    E3 = E2 * E
    E_P2 = E / (1.0 - E)
    
    SQRT_E = mathlib.sqrt(1 - E)
    _E = (1 - SQRT_E) / (1 + SQRT_E)
    _E2 = _E * _E
    _E3 = _E2 * _E
    _E4 = _E3 * _E
    _E5 = _E4 * _E
    
    M1 = (1 - E / 4 - 3 * E2 / 64 - 5 * E3 / 256)
    M2 = (3 * E / 8 + 3 * E2 / 32 + 45 * E3 / 1024)
    M3 = (15 * E2 / 256 + 45 * E3 / 1024)
    M4 = (35 * E3 / 3072)
    
    P2 = (3. / 2 * _E - 27. / 32 * _E3 + 269. / 512 * _E5)
    P3 = (21. / 16 * _E2 - 55. / 32 * _E4)
    P4 = (151. / 96 * _E3 - 417. / 128 * _E5)
    P5 = (1097. / 512 * _E4)
    
    R = 6378137
    
    ZONE_LETTERS = "CDEFGHJKLMNPQRSTUVWXX"
    
    
    def in_bounds(x, lower, upper, upper_strict=False):
        if upper_strict and use_numpy:
            return lower <= mathlib.min(x) and mathlib.max(x) < upper
        elif upper_strict and not use_numpy:
            return lower <= x < upper
        elif use_numpy:
            return lower <= mathlib.min(x) and mathlib.max(x) <= upper
        return lower <= x <= upper
    
    
    def check_valid_zone(zone_number, zone_letter):
        if not 1 <= zone_number <= 60:
            raise OutOfRangeError('zone number out of range (must be between 1 and 60)')
    
        if zone_letter:
            zone_letter = zone_letter.upper()
    
            if not 'C' <= zone_letter <= 'X' or zone_letter in ['I', 'O']:
                raise OutOfRangeError('zone letter out of range (must be between C and X)')
    
    
    def mixed_signs(x):
        return use_numpy and mathlib.min(x) < 0 and mathlib.max(x) >= 0
    
    
    def negative(x):
        if use_numpy:
            return mathlib.max(x) < 0
        return x < 0
    
    
    def to_latlon(easting, northing, zone_number, zone_letter=None, northern=None, strict=True):
        if not zone_letter and northern is None:
            raise ValueError('either zone_letter or northern needs to be set')
    
        elif zone_letter and northern is not None:
            raise ValueError('set either zone_letter or northern, but not both')
    
        if strict:
            if not in_bounds(easting, 100000, 1000000, upper_strict=True):
                raise OutOfRangeError('easting out of range (must be between 100.000 m and 999.999 m)')
            if not in_bounds(northing, 0, 10000000):
                raise OutOfRangeError('northing out of range (must be between 0 m and 10.000.000 m)')
        
        check_valid_zone(zone_number, zone_letter)
        
        if zone_letter:
            zone_letter = zone_letter.upper()
            northern = (zone_letter >= 'N')
    
        x = easting - 500000
        y = northing
    
        if not northern:
            y -= 10000000
    
        m = y / K0
        mu = m / (R * M1)
    
        p_rad = (mu +
                 P2 * mathlib.sin(2 * mu) +
                 P3 * mathlib.sin(4 * mu) +
                 P4 * mathlib.sin(6 * mu) +
                 P5 * mathlib.sin(8 * mu))
    
        p_sin = mathlib.sin(p_rad)
        p_sin2 = p_sin * p_sin
    
        p_cos = mathlib.cos(p_rad)
    
        p_tan = p_sin / p_cos
        p_tan2 = p_tan * p_tan
        p_tan4 = p_tan2 * p_tan2
    
        ep_sin = 1 - E * p_sin2
        ep_sin_sqrt = mathlib.sqrt(1 - E * p_sin2)
    
        n = R / ep_sin_sqrt
        r = (1 - E) / ep_sin
    
        c = _E * p_cos**2
        c2 = c * c
    
        d = x / (n * K0)
        d2 = d * d
        d3 = d2 * d
        d4 = d3 * d
        d5 = d4 * d
        d6 = d5 * d
    
        latitude = (p_rad - (p_tan / r) *
                    (d2 / 2 -
                     d4 / 24 * (5 + 3 * p_tan2 + 10 * c - 4 * c2 - 9 * E_P2)) +
                     d6 / 720 * (61 + 90 * p_tan2 + 298 * c + 45 * p_tan4 - 252 * E_P2 - 3 * c2))
    
        longitude = (d -
                     d3 / 6 * (1 + 2 * p_tan2 + c) +
                     d5 / 120 * (5 - 2 * c + 28 * p_tan2 - 3 * c2 + 8 * E_P2 + 24 * p_tan4)) / p_cos
    
        return (mathlib.degrees(latitude),
                mathlib.degrees(longitude) + zone_number_to_central_longitude(zone_number))
    
    
    def from_latlon(latitude, longitude, force_zone_number=None, force_zone_letter=None):
        #This function convert Latitude and Longitude to UTM coordinate
        #    
        #    Parameters
        #    ----------
        #    latitude: float
        #        Latitude between 80 deg S and 84 deg N, e.g. (-80.0 to 84.0)
        #
        #    longitude: float
        #        Longitude between 180 deg W and 180 deg E, e.g. (-180.0 to 180.0).
        #
        #    force_zone number: int
        #        Zone Number is represented with global map numbers of an UTM Zone
        #        Numbers Map. You may force conversion including one UTM Zone Number.
        #        More information see utmzones [1]_
        #
        #   .. _[1]: http://www.jaworski.ca/utmzones.htm
        
        if not in_bounds(latitude, -80.0, 84.0):
            raise OutOfRangeError('latitude out of range (must be between 80 deg S and 84 deg N)')
        if not in_bounds(longitude, -180.0, 180.0):
            raise OutOfRangeError('longitude out of range (must be between 180 deg W and 180 deg E)')
        if force_zone_number is not None:
            check_valid_zone(force_zone_number, force_zone_letter)
    
        lat_rad = mathlib.radians(latitude)
        lat_sin = mathlib.sin(lat_rad)
        lat_cos = mathlib.cos(lat_rad)
    
        lat_tan = lat_sin / lat_cos
        lat_tan2 = lat_tan * lat_tan
        lat_tan4 = lat_tan2 * lat_tan2
    
        if force_zone_number is None:
            zone_number = latlon_to_zone_number(latitude, longitude)
        else:
            zone_number = force_zone_number
    
        if force_zone_letter is None:
            zone_letter = latitude_to_zone_letter(latitude)
        else:
            zone_letter = force_zone_letter
    
        lon_rad = mathlib.radians(longitude)
        central_lon = zone_number_to_central_longitude(zone_number)
        central_lon_rad = mathlib.radians(central_lon)
    
        n = R / mathlib.sqrt(1 - E * lat_sin**2)
        c = E_P2 * lat_cos**2
    
        a = lat_cos * (lon_rad - central_lon_rad)
        a2 = a * a
        a3 = a2 * a
        a4 = a3 * a
        a5 = a4 * a
        a6 = a5 * a
    
        m = R * (M1 * lat_rad -
                 M2 * mathlib.sin(2 * lat_rad) +
                 M3 * mathlib.sin(4 * lat_rad) -
                 M4 * mathlib.sin(6 * lat_rad))
    
        easting = K0 * n * (a +
                            a3 / 6 * (1 - lat_tan2 + c) +
                            a5 / 120 * (5 - 18 * lat_tan2 + lat_tan4 + 72 * c - 58 * E_P2)) + 500000
    
        northing = K0 * (m + n * lat_tan * (a2 / 2 +
                                            a4 / 24 * (5 - lat_tan2 + 9 * c + 4 * c**2) +
                                            a6 / 720 * (61 - 58 * lat_tan2 + lat_tan4 + 600 * c - 330 * E_P2)))
    
        if mixed_signs(latitude):
            raise ValueError("latitudes must all have the same sign")
        elif negative(latitude):
            northing += 10000000
    
        return easting, northing, zone_number, zone_letter
    
    
    def latitude_to_zone_letter(latitude):
        # If the input is a numpy array, just use the first element
        # User responsibility to make sure that all points are in one zone
        if use_numpy and isinstance(latitude, mathlib.ndarray):
            latitude = latitude.flat[0]
    
        if -80 <= latitude <= 84:
            return ZONE_LETTERS[int(latitude + 80) >> 3]
        else:
            return None
    
    
    def latlon_to_zone_number(latitude, longitude):
        # If the input is a numpy array, just use the first element
        # User responsibility to make sure that all points are in one zone
        if use_numpy:
            if isinstance(latitude, mathlib.ndarray):
                latitude = latitude.flat[0]
            if isinstance(longitude, mathlib.ndarray):
                longitude = longitude.flat[0]
    
        if 56 <= latitude < 64 and 3 <= longitude < 12:
            return 32
    
        if 72 <= latitude <= 84 and longitude >= 0:
            if longitude < 9:
                return 31
            elif longitude < 21:
                return 33
            elif longitude < 33:
                return 35
            elif longitude < 42:
                return 37
    
        return int((longitude + 180) / 6) + 1
    
    
    def zone_number_to_central_longitude(zone_number):
        return (zone_number - 1) * 6 - 180 + 3
    
    ####

    return from_latlon
#end of 'return_from_latlon'

from_latlon = return_from_latlon()


"""


#See Autovaccum settings
#for row in fetchall_asyielderofdict("SELECT * FROM pg_settings  WHERE name LIKE '%vacuum%'",conn):
#    print(json.dumps(row))


#Check for the need for autovacuum to run..
#sql=""" -- adapted from https://dba.stackexchange.com/questions/21068/aggressive-autovacuum-on-postgresql/35234#35234
#    SELECT psut.relname as tablename,
#     to_char(psut.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum,
#     to_char(psut.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum,
#     to_char(pg_class.reltuples, '9G999G999G999') AS total_tuples, -- this is how many tuples are in the table
#     to_char(psut.n_dead_tup, '9G999G999G999') AS dead_tuples, -- this is how many tuples would be vacuumed away if a VACUUM was run
#     to_char(CAST(current_setting('autovacuum_vacuum_threshold') AS bigint)
#          + (CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric)
#            * pg_class.reltuples), '9G999G999G999') AS autovac_if_atleastthismany_deadtuples,
#     CASE
#         WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint)
#             + (CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric)
#                * pg_class.reltuples) < psut.n_dead_tup
#         THEN 'Autovacuum should kick in, there is enough dead tuples'
#         ELSE 'Autovacuum will not kick in, there is not enough dead tuples'
#     END AS expect_av
# FROM pg_stat_user_tables psut
#     JOIN pg_class on psut.relid = pg_class.oid
# ORDER BY 1;
#"""
#for row in fetchall_asyielderofdict(sql,conn):
#    print(json.dumps(row))



#Check database bloat: from https://wiki.postgresql.org/wiki/Show_database_bloat
#sql="""
#    SELECT
#      current_database(), schemaname, tablename, 
#      ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) AS tbloat,
#      CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END AS wastedbytes,
#      iname, 
#      ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) AS ibloat,
#      CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes
#    FROM (
#      SELECT
#        schemaname, tablename, cc.reltuples, cc.relpages, bs,
#        CEIL((cc.reltuples*((datahdr+ma-
#          (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::FLOAT)) AS otta,
#        COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
#        COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::FLOAT)),0) AS iotta -- very rough approximation, assumes all cols
#      FROM (
#        SELECT
#          ma,bs,schemaname,tablename,
#          (datawidth+(hdr+ma-(CASE WHEN hdr%ma=0 THEN ma ELSE hdr%ma END)))::NUMERIC AS datahdr,
#          (maxfracsum*(nullhdr+ma-(CASE WHEN nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
#        FROM (
#          SELECT
#            schemaname, tablename, hdr, ma, bs,
#            SUM((1-null_frac)*avg_width) AS datawidth,
#            MAX(null_frac) AS maxfracsum,
#            hdr+(
#              SELECT 1+COUNT(*)/8
#              FROM pg_stats s2
#              WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
#            ) AS nullhdr
#          FROM pg_stats s, (
#            SELECT
#              (SELECT current_setting('block_size')::NUMERIC) AS bs,
#              CASE WHEN SUBSTRING(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
#              CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma
#            FROM (SELECT version() AS v) AS foo
#          ) AS constants
#          GROUP BY 1,2,3,4,5
#        ) AS foo
#      ) AS rs
#      JOIN pg_class cc ON cc.relname = rs.tablename
#      JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema'
#      LEFT JOIN pg_index i ON indrelid = cc.oid
#      LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
#    ) AS sml
#    ORDER BY wastedbytes DESC
#"""
#for row in fetchall_asyielderofdict(sql.replace("%","%%"),conn):
#    print(row)
#return


#Extend postgres with ARBITRARY() aggregate; postgres doesn't have a FIRST() or LAST() aggregate (we could add those too)
#https://wiki.postgresql.org/wiki/First/last_(aggregate)
#try:
#    execute_sql("""
#        -- Create a function that always returns the first non-NULL item, this is FIRST() but we call it ARBITRARY()
#        CREATE FUNCTION public.arbitrary_agg ( anyelement, anyelement ) -- OR REPLACE
#        RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
#                SELECT $1;
#        $$;""",conn)
#    execute_sql("""     
#        -- And then wrap an aggregate around it
#        CREATE AGGREGATE public.ARBITRARY (
#                sfunc    = public.arbitrary_agg,
#                basetype = anyelement,
#                stype    = anyelement
#        );
#    """,conn)
#except Exception as ex:
#    if 'already exists with same argument types' in str(ex): #there's unfortunately no 'CREATE AGGREGATE IF NOT EXISTS' or "CREATE OR REPLACE AGGREGATE" :(
#        pass
#    else:
#        raise ex
#commit(conn)




#How to do fast spatial stuff (once you've done execute_sql("CREATE EXTENSION postgis;", conn)
###############################################################################################
#
#1. Create your tables with a spatial column, best enforce an SRID on it
#create table if not exists temp_blendfiltering(...
# 
# enforce MGA zone 55 (EPSG 28355), which is good for all of Qld (you can work +/- 1 zone pretty ok), as PostGIS's ST_Distance/ST_DWithin value is in units from spatial ref. We can do this one of two ways
# way 1:
# "spatialobj geometry,"  #create a spatial column
# "CONSTRAINT enforce_srid_spatialobj  CHECK (st_srid(spatialobj) = 28355)," #ensure we don't accidentially insert anything not in MGZ Zone 55
#
# way 2:
# "spatialobj geometry(GEOMETRY,28355),"  #GEOMETRY means any geometry type, rather than just LineString's or whatever
#
#2. Set the spatial column to external storage and index it using a GIST
#execute_sql("ALTER  TABLE temp_blendfiltering ALTER COLUMN spatialobj  SET STORAGE EXTERNAL",conn) #This gives a minor speedup. It ensures postgis stores geometry UNCOMPRESSED so avoids query-time decompression which can be slow. http://blog.cleverelephant.ca/2018/09/postgis-external-storage.html
#execute_sql("CREATE INDEX temp_blendfiltering_gidx ON temp_blendfiltering USING GIST (spatialobj)",conn) #GIST to produce index of bounding box generalised search index. Could try SPGIST as well
#
#3. Populate the table
#e.g.
#            recstoinsert.append( {
#                'pk_uid': feature['properties']['tmr_record_uid'],
#                'spatialobj': "SRID=28355;"+feature['properties']['wkt'] #NB: prepending SRID to wkt like this to form an EWKT is a PostGIS feature the PostGIS docs say may be deprecated at any time
#                #'spatialobj': feature['properties']['wkt'] #won't work
#                #wkt property is from prep_geojson_for_athena(), is MGAz55.  Can do ::geometry to cast
#                #ST_SetSRID(.. ,28355)
#                #ST_GeometryFromText(wkt,4326 for wgs84 else 28355 for MGA zone 55)
#                #ST_GeomFromText(wkt,28355)
#                #NB Postgis docs also confusingly and contradictorily say: "Regardless which spatial reference system you use, the units returned by the measurement (ST_Distance, ST_Length, ST_Perimeter, ST_Area) and for input of ST_DWithin are in meters.""
#
#4. Consider calling ST_Simplify() to generalize the geometry. Note the 'true' at the end, this is important!
#    execute_sql("update temp_blendfiltering set spatialobj=ST_Simplify(spatialobj,10,true)",conn) 
#
#5. Cluster the table on the spatial column
#    #cluster on the spatial index. needs a lock
#    # "When a table is clustered, it is physically reordered based on the index information. Clustering is a one-time operation: when the table is subsequently updated, the changes are not clustered. That is, no attempt is made to store new or updated rows according to their index order. (If one wishes, one can periodically recluster by issuing the command again. Also, setting the table's FILLFACTOR storage parameter to less than 100% can aid in preserving cluster ordering during updates, since updated rows are kept on the same page if enough space is available there.)"
#    execute_sql("CLUSTER temp_blendfiltering USING temp_blendfiltering_gidx", conn) #Rewrites the table. "Geometries that are near each other in space are near each other on disk." (i.e. in same page)
#    #need to do an ANALYSE next, to take advantage of that clustering
#
#6. ANALYSE (might as well VACUUM too)
#
#    #Do a VACUUM ANALYSE on our repopulated and clustered table (has to be done outside of transaction)
#    execute_sql_outsideoftransaction_afterdoingacommit("vacuum analyze temp_blendfiltering",conn)
#    
#
#7. Consider using a read replica to run the remaining query
#
#
#8. Don't use CTE ('with') if you want a spatial index to be used, as the resultant temporary table won't have one!
#    #nb: Postgis (<v12) won't pushdown predicated into WITHs:
#    # "A useful property of WITH queries is that they are evaluated only once per execution of the parent query, even if they are referred to more than once by the parent query or sibling WITH queries. Thus, expensive calculations that are needed in multiple places can be placed within a WITH query to avoid redundant work. Another possible application is to prevent unwanted multiple evaluations of functions with side-effects. 
#    #  However, the other side of this coin is that the optimizer is less able to push restrictions from the parent query down into a WITH query than an ordinary sub-query. 
#    #  The WITH query will generally be evaluated as stated, without suppression of rows that the parent query might discard afterwards"
#    #But this is ok for us as we are just doing a batch process and there's no savings for us to have if predicates were pusheddown
#
#    #It seems that the temporary tables which result from a 'with' don't have (spatial, at least) indexes: in <v12 postgres, 'with' results in actual temp tables being created and I guess postgres
#    #doesn't transfer across the request for the spatial index..
#    #This is true even if the 'with' is trivial, e.g.:
#    # with ttt as (
#    #            select * from temp_blendfiltering
#    #        )
#    #    ,
#    # ..same sql but with 'temp_blendfiltering' replaced with 'ttt'... is now super-slow spatially...ST_DWithin runs 20x more slowly..
#    
#    #NOTE: in Postgres (<v12, @12 you can use the 'materialized' keyword to control), CTE/'with' blocks are evaluated in full and without any predicate or column push-down from the surrounding context.
#    #Not sure about athena, whether it works the same way or not..
#    
# 
#9.  for spatial proximity, best to use postgis's 'ST_Dwithin()'
#    # "Pre 1.3, ST_Expand was used in conjunction with distance to do indexable queries. 
#    #  Something of the form the_geom && ST_Expand('POINT(10 20)', 10) AND ST_Distance(the_geom, 'POINT(10 20)') < 10 
#    #  Post 1.2, this was replaced with the easier ST_DWithin construct."
#    # ("The && operator itself doesn't guarantee that the geometries themselves intersect, just their bounding boxes")
 
 



#Suggested database setup
#########################
#As master user:
##create writer
#execute_sql("create role writer LOGIN PASSWORD 'writer9876_asdpoi';",conn)
#execute_sql("CREATE EXTENSION postgis;", conn) #enable spatial extensions for the database
#commit(conn)
##create a near-readonly reader: they can create their own tables but can't edit other tables
#execute_sql("CREATE USER reader LOGIN PASSWORD 'reader543_yuizz';",conn)
#execute_sql("GRANT CONNECT ON DATABASE "+db_name+" TO reader; ",conn)
#execute_sql("GRANT USAGE ON SCHEMA public TO reader;",conn)
#execute_sql("GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader; ",conn)
#execute_sql("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO reader;",conn) 
#commit(conn)

##Then connect to the server again, once as each user that have or will create tables (in this case, that is just as 'writer') and do:
#execute_sql("GRANT USAGE ON SCHEMA public TO reader; ",conn)
#execute_sql("GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader;",conn)
#execute_sql("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO reader;",conn)
#commit(conn)




