#Date:20200213

#athean_qry : takes as qury and sennds to athean, retuning result
#todo: change functionality or wait times instead for looping retri9es

import boto3
import time

def athena_qry(query_string, athena_db, S3_output_qry, retry_count):
    '''
    lambda athena interaction
    requires parmaters- query string (e.g 'Select * from tbl') 
                        athena tbls config and repo set as global variables e.g (i.e tbl = incidents, repo = s3://bukect/qry)
    
    '''

    #query and save to s3
    # athena client 
    client = boto3.client('athena')
    # Execution
    response = client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={
            'Database': athena_db
        },
        ResultConfiguration={
            'OutputLocation': S3_output_qry,
        }
    )
    
    #get query execution id - kdy in s3
    query_execution_id = response['QueryExecutionId']

    # get execution status - awating succesfull response, todo: rewrite below to wait based on time rather than current loop 
    for i in range(1, 1 + retry_count):

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
    athena_result = client.get_query_results(QueryExecutionId=query_execution_id)
    
    return athena_result,query_execution_id