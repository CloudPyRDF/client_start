import json
import boto3



def start_lambdas(conf_file):
    
    #Read data from configuration file
    f = open(conf_file, "r")
    
    eos_login = f.readline()
    eos_password = f.readline()

    f.readline()
    
    aws_access_key = f.readline()
    aws_secret_key = f.readline()
    aws_session_token = f.readline()
    
    f.readline()

    funs = []
    while((line = f.readline()) != '/n'):
	funs.append(line)

    eos = []
    while((line = f.readline()) != '/n'):
	eos.append(line)

    bucket_names = []
    while((line = f.readline()) != '/n'):
	bucket_names.append(line)

    object_keys = []
    while((line = f.readline()) != '/n'):
	object_keys.append(line)

    filenames = []
    while((line = f.readline()) != '/n'):
	filenames.append(line)
    
    f.close()


    client = boto3.client('lambda',
    aws_access_key_id = aws_access_key,
    aws_secret_access_key = aws_secret_key,
    aws_session_token = aws_session_token)

    for eos_url, bucket_name, object_key in eos, bucket_names, object_keys:
        client.invoke(
            FunctionName = "eos_lambda",
            InvocationType = "Event",
            Payload = {
                path: "eos_url",
                login: login,
                password: password,
                bucket_name: bucket_name,
                object_key: object_key
                
            }
        )
    
    for bucket_name, object_key in bucket_names, object_keys:
        s3_path = "https://s3.us-east-1.amazonaws.com/" + bucket_name/ + object_key
        client.invoke(
            FunctionName = "root_lambda".
            InvocationType = "Event",
            Payload = {
                s3_path: s3_path,
            }
        )
    
    result_list = []
    
    for fun, bucket_name, object_key, file_path in funcs, bucket_names, object_keys, filenames:
        in_bucket_name = "https://s3.us-east-1.amazonaws.com/" + bucket_name + "/" + key_name
        out_bucket_name = "https://s3.us-east-1.amazonaws.com/" + bucket_name + "/" + key_name + "/result"
        result_list.append(client.invoke(
                FunctionName = "map_lambda",
                InvocationType = "Event",
                Payload = {
                    in_bucket_name: in_bucket_name,
                    out_bucket_name: out_bucket_name,
                    script: fun,
                    file_path: file_path
                }
        ))
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }
