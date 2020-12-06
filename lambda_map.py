from utils import invoke_lambda, EOS_LAMBDA_NAME, ROOT_LAMBDA_NAME, wait_for_completion


def lambda_map(conf, client):
    print(conf)
    s3 = conf["s3"]
    eos = conf["eos"]
    script = conf["script"]
    eos_cred = eos["credentials"]

    for eos_path, eos_file, object_key in zip(eos["paths"], eos["filenames"], s3["files"]):
        print(eos_path, eos_file, object_key)
        invoke_lambda(
            client,
            name=EOS_LAMBDA_NAME,
            payload={
                "eos_path": eos_path,
                "eos_filename": eos_file,
                "eos_login": eos_cred["login"],
                "eos_password": eos_cred["password"],
                "s3_bucket_name": s3["buckets"]["input"],
                "s3_object_key": object_key
            })

    if not wait_for_completion(client, EOS_LAMBDA_NAME):
        exit(1)

    for object_key in s3["files"]:
        invoke_lambda(
            client,
            name=ROOT_LAMBDA_NAME,
            payload={
                "in_bucket": s3["buckets"]["input"],
                "in_bucket_file_path": object_key,
                "script": script,
                "out_bucket": s3["buckets"]["output"],
                "out_bucket_file_path": object_key
            })

    if not wait_for_completion(client, ROOT_LAMBDA_NAME):
        exit(1)
