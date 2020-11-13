import json
import boto3
import time


def wait_for_complition(function_name):

    while True:
        if client.get_function(FunctionName=function_name)['Configuration']['State'] == 'Inactive':
            break
        
        if client.get_function(FunctionName=function_name)['Configuration']['State'] == 'Failed':
            raise Exception("Function failed!!!") 
            
        time.sleep(1)    

def start_lambdas(conf_file):
    
    #Read data from configuration file
    conf = json.loads(conf_file)

    client = boto3.client('lambda')

    for eos_path, eos_file, bucket_name, object_key in conf['eos_paths'], conf['eos_filenames'], conf['s3_bucket_names'], conf['s3_object_keys']:
        client.invoke(
            FunctionName = "eos_lambda",
            InvocationType = "Event",
            Payload = {
                eos_path: eos_url,
                eos_filename: eos_file,
                eos_login: conf['credentials']['login'],
                eos_password: conf['credentials']['password'],
                s3_bucket_name: bucket_name,
                s3_object_key: object_key
                
            }
        )
    
    wait_for_complition("eos_lambda")
    
    
    for script, in_bucket_name, object_key, out_bucket_name in conf['scripts'], conf['s3_bucket_names'], conf['s3_object_keys'], conf['s3_out_bucket_names']:
        client.invoke(
            FunctionName = "root_lambda",
            InvocationType = "Event",
            Payload = {
              in_bucket: in_bucket_name,
              in_bucket_file_path: object_key,

              script: script,

              out_bucket:out_bucket_name,
              out_bucket_file_path: object_key
}
        )
    
    wait_for_complition("root_lambda")
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }
