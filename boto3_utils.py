import boto3

def get_files_count(bucket_name):

    client - boto3.clinet('s3')
    
    response = client.list_objects_v2(bucket_name)
    return response['KeyCount']
    
    # wersja jakby w buckecie było > 1000 objectów
    '''s3 = boto3.resource('s3')
    
    bucket = s3.Bucket(bucket_name)
    obj_list = bucket.objects.all()
    
    return len(obj_list)'''    


def is_file_in_bucket(bucket_name, file_name):
    
    client = boto3.client('s3')
    results = client.list_objects_v2(Bucket=bucket_name, Prefix=file_name)
    
    if len(results['Contents']) > 0:
        return true
    else:
        return false
    
    # słaboefektywne, ale jeśli tamto się wykrzaczy na jakimś prefiksie(jakimś cudem bedą dwa pliki, gdzie nazwa jednego będzie prefiksem 2)...
    '''s3 = boto3.resource('s3')
    
    bucket = s3.Bucket(bucket_name)
    obj_list = bucket.objects.all()
    
    return s3.Object(bucket_name, file_name) in obj_list'''