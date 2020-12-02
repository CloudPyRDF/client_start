import json
import boto3
import time
import uproot


def wait_for_complition(function_name):

    while True:
        if client.get_function(FunctionName=function_name)['Configuration']['State'] == 'Inactive':
            break
        
        if client.get_function(FunctionName=function_name)['Configuration']['State'] == 'Failed':
            raise Exception("Function" + function_name + "failed!!!") 
            
        time.sleep(1)
        
def map(login, password, scripts, eos_paths, eos_filenames, s3_buckets, s3_object_keys, client):

    for eos_path, eos_file, object_key in eos_paths, eos_filenames, s3_object_keys:
        client.invoke(
            FunctionName = "eos_lambda",
            InvocationType = "Event",
            Payload = {
                eos_path: eos_path,
                eos_filename: eos_file,
                eos_login: login,
                eos_password: password,
                s3_bucket_name: s3_buckets[0],
                s3_object_key: object_key
            }
        )
    
    try:
        wait_for_complition("eos_lambda")
    except:
        print("Function eos_lambda failed, exiting program...")
        exit(1)
    
    
    for script_file_name, object_key in scripts, s3_object_keys:
        script_file = open(script_file_name, "r")
        script = script_file.read()
        script_file.close()
        client.invoke(
            FunctionName = "root_lambda",
            InvocationType = "Event",
            Payload = {
              in_bucket: s3_buckets[0],
              in_bucket_file_path: object_key,
              script: script,
              out_bucket: s3_buckets[1],
              out_bucket_file_path: object_key
            }
        )
        
    try:
        wait_for_complition("root_lambda")
    except:
        print("Function root_lambda failed, exiting program...")
        exit(1)
 
def extract_from_root(res_file, s3_object_keys):
    #TODO
 
def reduce(bucket, s3_object_keys):
    
    client = boto3.clinet('s3')
    
    for object_key in s3_object_keys:
        file_path = "./" + object_key
        client.download_file(bucket, object_key, file_path)

    res_file = uproot.create("./result.root")
    
    extract_from_root(res_file, s3_object_keys)
    
    res_file.close()

def start_lambdas(conf_file, s3_buckets):
    
    #Read data from configuration file
    conf = json.loads(conf_file)
    
    login = conf['credentials']['login']
    password = conf['credentials']['password']
    scripts = conf['scripts']
    eos_paths = conf['eos_paths']
    eos_filenames = conf['eos_filenames']
    s3_object_keys = conf['s3_object_keys']

    client = boto3.client('lambda')

    map(login, password, scripts, eos_paths, eos_filenames, s3_buckets, s3_object_keys, client)
    
    reduce(s3_buckets[1], s3_object_keys)
    
    