import argparse
import pandas as pd
from functions.aws_utils import *

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
    
    preprocess_task_definition_arn = deployer.deploy(
        repo_name="datathon2025",  # ECR repository name
        image_name="preprocess",  # Docker image name
        dockerfile_path="./Dockerfiles/Dockerfile.preprocess",
        task_name="datathon-preprocess",  # ECS task definition name
        cluster_name="datathon2025-cluster"  # ECS cluster name
    )

    model_task_definition_arn = deployer.deploy(
        repo_name="datathon2025",  # ECR repository name
        image_name="preprocess",  # Docker image name
        dockerfile_path="./Dockerfiles/Dockerfile.model",
        task_name="datathon-model",  # ECS task definition name
        cluster_name="datathon2025-cluster"  # ECS cluster name
    )

    neo4j_task_definition_arn = deployer.deploy(
        repo_name="datathon2025",  # ECR repository name
        image_name="preprocess",  # Docker image name
        dockerfile_path="./Dockerfiles/Dockerfile.neo4j",
        task_name="datathon-neo4j",  # ECS task definition name
        cluster_name="datathon2025-cluster"  # ECS cluster name
    )

    print(task_definition_arn)
    cluster_name='datathon2025-cluster'
    subnet_id='subnet-04676f82c6c4baf59'

    state_machine_definition = f'''
    {{
    "Comment": "State Machine to run an ECS task",
    "StartAt": "RunEcsTask",
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
            "Next": RunEcsTask_Model
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
            "Next": RunEcsTask_Neo4j
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
        }},
    }}
    }}'''

    # Initialize Step Function
    sf = StepFunction(aws_access_key_id=AWS_ACCESS_KEY_ID, 
                   aws_secret_access_key=AWS_SECRET_KEY)
    
    state_machine_arn = sf.create_step_function(state_machine_definition,
                            'arn:aws:iam::484907528704:role/StepFunctionsExecutionRole')
    
    sf.start_step_function_execution(state_machine_arn)

    # Deploy container for lambda function to run pipeline
    task_definition_arn = deployer.deploy(
        repo_name="datathon2025",
        image_name="runpipeline",
        dockerfile_path="./Dockerfiles/Dockerfile.runpipeline",
        task_name="datathon-runpipeline",
        cluster_name="datathon2025-cluster"
    )