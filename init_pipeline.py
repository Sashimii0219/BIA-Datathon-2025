import argparse
import pandas as pd
from functions.aws_utils import *
import json

from dotenv import load_dotenv
import os

# Create argument parser
parser = argparse.ArgumentParser(description="Process some inputs.")

# Define arguments
parser.add_argument(
    "-m",
    "--method",
    type=str,
    default='relik',
    help="Currently Available Methods: relik, mrebel"
    )

# Parse arguments
args = parser.parse_args()
method = args.method

if __name__ == "__main__":

    load_dotenv()
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
    
    deployer = ECS(aws_access_key_id=AWS_ACCESS_KEY_ID, 
                   aws_secret_access_key=AWS_SECRET_KEY)
    
    sf = StepFunction(aws_access_key_id=AWS_ACCESS_KEY_ID, 
                      aws_secret_access_key=AWS_SECRET_KEY)
    
    s3 = S3(aws_access_key_id=AWS_ACCESS_KEY_ID, 
            aws_secret_access_key=AWS_SECRET_KEY)
    
    repo_name = 'datathon2025'
    cluster_name='datathon2025-cluster'
    subnet_id='subnet-04676f82c6c4baf59'
    state_machine_name = "Datathon2025StateMachine"

    preprocess_task_definition_arn = deployer.deploy(
        repo_name=repo_name,  # ECR repository name
        image_name="preprocess",  # Docker image name
        dockerfile_path="./Dockerfiles/Dockerfile.preprocess",
        task_name="datathon-preprocess",  # ECS task definition name
        cluster_name=cluster_name  # ECS cluster name
    )

    model_task_definition_arn = deployer.deploy(
        repo_name=repo_name,
        image_name="model",
        dockerfile_path="./Dockerfiles/Dockerfile.model",
        task_name="datathon-model",
        cluster_name=cluster_name
    )

    validation_task_definition_arn = deployer.deploy(
        repo_name=repo_name,
        image_name="validation",
        dockerfile_path="./Dockerfiles/Dockerfile.validation",
        task_name="datathon-validation", 
        cluster_name=cluster_name
    )

    neo4j_task_definition_arn = deployer.deploy(
        repo_name=repo_name,
        image_name="neo4j",
        dockerfile_path="./Dockerfiles/Dockerfile.neo4j",
        task_name="datathon-neo4j", 
        cluster_name=cluster_name
    )

    state_machine_definition = f'''
    {{
    "Comment": "State Machine to run an ECS task",
    "StartAt": "RunEcsTask_Preprocess",
    "States": {{
        "RunEcsTask_Preprocess": {{
            "Type": "Task",
            "Resource": "arn:aws:states:::ecs:runTask.sync",
            "Parameters": {{
                "Cluster": "{cluster_name}",
                "TaskDefinition": "{preprocess_task_definition_arn}",
                "LaunchType": "FARGATE",
                "NetworkConfiguration": {{
                    "AwsvpcConfiguration": {{
                        "Subnets": ["{subnet_id}"],
                        "AssignPublicIp": "ENABLED"
                    }}
                }}
            }},
            "Next": "RunEcsTask_Model"
        }},
        "RunEcsTask_Model": {{
            "Type": "Task",
            "Resource": "arn:aws:states:::ecs:runTask.sync",
            "Parameters": {{
                "Cluster": "{cluster_name}",
                "TaskDefinition": "{model_task_definition_arn}",
                "LaunchType": "FARGATE",
                "NetworkConfiguration": {{
                    "AwsvpcConfiguration": {{
                        "Subnets": ["{subnet_id}"],
                        "AssignPublicIp": "ENABLED"
                    }}
                }}
            }},
            "Next": "RunEcsTask_Validation"
        }},
        "RunEcsTask_Validation": {{
            "Type": "Task",
            "Resource": "arn:aws:states:::ecs:runTask.sync",
            "Parameters": {{
                "Cluster": "{cluster_name}",
                "TaskDefinition": "{validation_task_definition_arn}",
                "LaunchType": "FARGATE",
                "NetworkConfiguration": {{
                    "AwsvpcConfiguration": {{
                        "Subnets": ["{subnet_id}"],
                        "AssignPublicIp": "ENABLED"
                    }}
                }}
            }},
            "Next": "RunEcsTask_Neo4j"
        }},
        "RunEcsTask_Neo4j": {{
            "Type": "Task",
            "Resource": "arn:aws:states:::ecs:runTask.sync",
            "Parameters": {{
                "Cluster": "{cluster_name}",
                "TaskDefinition": "{neo4j_task_definition_arn}",
                "LaunchType": "FARGATE",
                "NetworkConfiguration": {{
                    "AwsvpcConfiguration": {{
                        "Subnets": ["{subnet_id}"],
                        "AssignPublicIp": "ENABLED"
                    }}
                }}
            }},
            "End": true
        }}
    }}
    }}'''

    # Initialize Step Function
    sf = StepFunction(aws_access_key_id=AWS_ACCESS_KEY_ID, 
                   aws_secret_access_key=AWS_SECRET_KEY)
    
    state_machine_arn = sf.create_step_function(state_machine_name, state_machine_definition,
                            'arn:aws:iam::484907528704:role/StepFunctionsExecutionRole')
    print(state_machine_arn)
    # Deploy container for lambda function to run pipeline
    task_definition_arn = deployer.deploy(
        repo_name=repo_name,
        image_name="runpipeline",
        dockerfile_path="./Dockerfiles/Dockerfile.runpipeline",
        task_name="datathon-runpipeline",
        cluster_name=cluster_name
    )

    var_name = {
        "repo_name" : repo_name,
        "cluster_name" : cluster_name,
        "state_machine_name" : state_machine_name,
        "state_machine_arn" : state_machine_arn    
        }
    
    # Save configs to config folder
    var_json = json.dumps(var_name, indent=4)
    s3.upload_to_s3("datathon2025",
                    "configs",
                    "sf_vars.json",
                    var_json,
                    df=False)
    
    state_machine_definition_json = json.dumps(state_machine_definition, indent=4)
    s3.upload_to_s3("datathon2025",
                    "configs",
                    "state_machine_definition.json",
                    state_machine_definition_json,
                    df=False)