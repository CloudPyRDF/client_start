import json
import time

EOS_LAMBDA_NAME = "eos_lambda"
INIT_LAMBDA_NAME = "init_lambda"
SPLIT_LAMBDA_NAME = "split_lambda"
ROOT_LAMBDA_NAME = "root_lambda"
REDUCE_LAMBDA_NAME = "reduce_lambda"


def wait_for_completion(client, function_name):
    try:
        while True:
            state = client.get_function(FunctionName=function_name, Qualifier='$LATEST')['Configuration']
            # ["StateReason"]
            # state = client.get_provisioned_concurrency_config(FunctionName=function_name, Qualifier="$LATEST")
            if state == 'Inactive':
                return True
            if state == 'Failed':
                raise RuntimeError("Function" + function_name + "failed!!!")
            time.sleep(1)

    except RuntimeError as ex:
        print("Error: ", ex)
        return False


def invoke_lambda(client, name, payload, synchronous=True):
    return client.invoke(
        FunctionName=name,
        # InvocationType="RequestResponse" if synchronous else "Event",
        Payload=bytes(
            json.dumps(payload),
            encoding='utf8'
        ))


def read_terraform_config(terraform_conf_file):
    with open(terraform_conf_file) as config:
        terraform_output = json.load(config)

    buckets = {
        "input": terraform_output["input_bucket_arn"]["value"].split(":")[-1],
        "output": terraform_output["output_bucket_arn"]["value"].split(":")[-1],
    }

    return buckets
