import json
import os

import boto3

from lambda_map import lambda_map
from lambda_reduce import lambda_reduce
from utils import read_terraform_config


def read_file(filename):
    with open(filename, "r") as file:
        return file.read()


def read_config(user_conf_file, terraform_conf_file):
    with open(user_conf_file) as config:
        conf = json.load(config)

    # eos files
    eos_paths = []
    eos_filenames = []
    for path in conf['eos_paths']:
        path, filename = os.path.split(path)
        eos_paths.append(path + "/")
        eos_filenames.append(filename)

    # script to execute
    script = read_file(conf['script'])
    s3_buckets = read_terraform_config(terraform_conf_file)
    configuration = {
        "eos": {
            "credentials": {
                "login": conf['credentials']['login'],
                "password": conf['credentials']['password']
            },
            "paths": eos_paths,
            "filenames": eos_filenames,
        },
        "s3": {
            "files": conf['s3_object_keys'],
            "buckets": s3_buckets
        },
        "script": script
    }
    return configuration


def run_workflow(json_conf_file, terraform_conf_file):
    configuration = read_config(json_conf_file, terraform_conf_file)
    client = boto3.client('lambda')

    lambda_map(configuration, client)
    result = lambda_reduce(configuration, client)
    return result


run_workflow("config_file.json", "terraform_config")
