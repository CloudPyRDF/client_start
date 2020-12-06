import concurrent.futures

from utils import invoke_lambda, EOS_LAMBDA_NAME, ROOT_LAMBDA_NAME


def invoke_eos_lambda(client, eos_path, eos_file, login, password, s3_input, object_key):
    return invoke_lambda(
        client,
        name=EOS_LAMBDA_NAME,
        payload={
            "eos_path": eos_path,
            "eos_filename": eos_file,
            "eos_login": login,
            "eos_password": password,
            "s3_bucket_name": s3_input,
            "s3_object_key": object_key,
            "url":"https://eospublichttp01.cern.ch/eos/"
        })


def invoke_root_lambda(client, in_bucket, object_key, script, out_bucket):
    return invoke_lambda(
        client,
        name=ROOT_LAMBDA_NAME,
        payload={
            "in_bucket_name": in_bucket,
            "file_path": object_key,
            "script": script,
            "out_bucket_name": out_bucket,
        })


def lambda_map(conf, client):
    print(conf)
    s3 = conf["s3"]
    eos = conf["eos"]
    script = conf["script"]
    eos_cred = eos["credentials"]

    pool = concurrent.futures.ThreadPoolExecutor(64)
    futures_list = []

    # for eos_path, eos_file, object_key in zip(eos["paths"], eos["filenames"], s3["files"]):
    #     print(eos_path, eos_file, object_key)
    #     result=invoke_eos_lambda(client, eos_path, eos_file, eos_cred["login"], eos_cred["password"],
    #                       s3["buckets"]["input"], object_key)
    #     print("downloaded this mofo")
    #     print(result)
    #     fut = pool.submit(invoke_eos_lambda, client, eos_path, eos_file, eos_cred["login"], eos_cred["password"],
    #                       s3["buckets"]["input"], object_key)
    #     futures_list.append(fut)

    # happy case
    # result=concurrent.futures.wait(futures_list)
    # print("downloaded this mofo")
    # print(result)
    # '''if not wait_for_completion(client, EOS_LAMBDA_NAME):
    #     exit(1)'''

    futures_list = []
    for object_key in s3["files"]:
        result=invoke_root_lambda(client, s3["buckets"]["input"], object_key, script, s3["buckets"]["output"])
        print("processed this mofo")
        print(result)
        # fut = pool.submit(invoke_root_lambda, client,
        #                   s3["buckets"]["input"],
        #                   object_key,
        #                   script,
        #                   s3["buckets"]["output"])
        # futures_list.append(fut)

    # happy case
    # result=concurrent.futures.wait(futures_list)

    '''if not wait_for_completion(client, ROOT_LAMBDA_NAME):
        exit(1)'''
