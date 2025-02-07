import pandas as pd
from functions.aws_utils import *

from dotenv import load_dotenv
import os

if __name__ == "__main__":

    load_dotenv()
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
    
    deployer = ECS(aws_access_key_id=AWS_ACCESS_KEY_ID, 
                   aws_secret_access_key=AWS_SECRET_KEY)
    
    task_definition_arn = deployer.deploy(
        repo_name="datathon2025",  # ECR repository name
        image_name="runpipeline",  # Docker image name
        dockerfile_path="./Dockerfiles/Dockerfile.runpipeline",
        task_name="datathon-runpipeline",  # ECS task definition name
        cluster_name="datathon2025-cluster"  # ECS cluster name
    )

