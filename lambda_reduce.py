import boto3

from utils import invoke_lambda, REDUCE_LAMBDA_NAME


def lambda_reduce(configuration, client):
    s3_object_keys = configuration["s3"]["files"]
    s3_output_bucket = configuration["s3"]["buckets"]["output"]

    s3 = boto3.client('lambda')

    result_fname = invoke_lambda(
        client,
        name=REDUCE_LAMBDA_NAME,
        payload={
            "s3_object_keys": s3_object_keys,
            "in_bucket_name": s3_output_bucket,
            "out_bucket_name": s3_output_bucket,
            "out_file_path": "out.root"
        },
        synchronous=True
    )
    result = s3.download_file(s3_output_bucket, "out.root", result_fname["file_name"])

    return result
