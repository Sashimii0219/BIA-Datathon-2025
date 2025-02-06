import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from dotenv import load_dotenv
import subprocess
import os
from io import StringIO
import pandas as pd


# Connection class
class AWSConnection:
    def __init__(self, aws_access_key_id, aws_secret_key, region_name='ap-southeast-1'):

        try:
            self.session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
            )
            print("Connection to S3 Established!")
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"Error: {e}")
            self.session = None

    # Retrieve client for particular service E.g. S3, ECS, EC2
    def get_client(self, client_name):
        if self.session:
            return self.session.client(client_name)
        else:
            raise Exception("AWS session not initialized properly.")

# S3 Service Class
class S3:
    def __init__(self, aws_access_key_id, aws_secret_access_key):
        aws_connection = AWSConnection(aws_access_key_id, aws_secret_access_key)
        self.s3 = aws_connection.get_client("s3")

    
    # List all buckets
    def list_buckets(self):
        try:
            # List S3 buckets
            response = self.s3.list_buckets()
            return response.get('Buckets', [])
        except Exception as e:
            print(f"Error accessing S3: {e}")
            return []
        

    # List all folders in specific buckets
    def list_folders(self, bucket_name):
        # List objects with a prefix
        response = self.s3.list_objects_v2(Bucket=bucket_name, Delimiter="/")

        # Extract folder names from CommonPrefixes
        folders = [prefix["Prefix"] for prefix in response.get("CommonPrefixes", [])]

        print("Folders:", folders)


    # Upload files to S3
    def upload_to_s3(self, bucket_name, prefix, file_name, df):

        # file_path e.g. clean_data/

        # Create an in-memory buffer
        csv_buffer = StringIO()

        # Write the df to the buffer
        df.to_csv(csv_buffer, index=False)

        s3_file_path = f"{prefix}/{file_name}"

        try:
            # Uploading file
            self.s3.put_object(Bucket=bucket_name, Key=s3_file_path, Body=csv_buffer.getvalue())
            print(f"CSV file '{file_name}' has been uploaded to S3 at '{s3_file_path}'")

        except Exception as e:
            print(f"Error uploading CSV to S3: {e}")



    # Read file from S3
    def read_from_s3(self, bucket_name, file_path):
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=file_path)
            # Read the CSV file directly from S3 into a DataFrame
            df = pd.read_csv(response['Body'])  # 'Body' contains the file content
            
            print(f"CSV file from {file_path} successfully loaded into DataFrame.")
            return df
        
        except Exception as e:
            print(f"Error reading CSV from S3: {e}")
            return None


class ECS:
    def __init__(self, aws_access_key_id, aws_secret_access_key):
        aws_connection = AWSConnection(aws_access_key_id, aws_secret_access_key)
        self.ecs_client = aws_connection.get_client("ecs")
        self.ecr_client = aws_connection.get_client("ecr")
    
    def create_ecr_repository(self, repo_name):
        """Create an ECR repository if it doesn't exist."""
        try:
            response = self.ecr_client.create_repository(repositoryName=repo_name)
            print(f"Repository {repo_name} created.")
        except ClientError as e:
            print(f"Error creating repository: {e}")
            return None
        return response['repository']['repositoryUri']
    
    def build_docker_image(self, image_name, dockerfile_path='./Dockerfiles'):
        """Build the Docker image using the Dockerfile."""
        print(f"Building Docker image: {image_name}")
        try:
            subprocess.check_call(['docker', 'build', '-t', image_name, dockerfile_path])
        except subprocess.CalledProcessError as e:
            print(f"Error building Docker image: {e}")
            return False
        return True
    
    def push_to_ecr(self, image_name, ecr_repo_uri):
        """Tag the Docker image and push it to ECR."""
        try:
            # Login to ECR
            print("Logging into ECR...")
            subprocess.check_call(['aws', 'ecr', 'get-login-password', '--region', self.region, '|', 'docker', 'login', '--username', 'AWS', '--password-stdin', ecr_repo_uri])

            # Tag the image
            docker_tag = f"{ecr_repo_uri}:{image_name}"
            subprocess.check_call(['docker', 'tag', image_name, docker_tag])

            # Push to ECR
            subprocess.check_call(['docker', 'push', docker_tag])
            print(f"Image pushed to {docker_tag}")
        except subprocess.CalledProcessError as e:
            print(f"Error pushing image to ECR: {e}")
            return False
        return True
    
    def register_task_definition(self, task_name, image_uri, cpu='256', memory='512', container_name='container'):
        """Register a new ECS Task Definition."""
        try:
            response = self.ecs_client.register_task_definition(
                family=task_name,
                cpu=cpu,
                memory=memory,
                networkMode='awsvpc',
                containerDefinitions=[
                    {
                        'name': container_name,
                        'image': image_uri,
                        'essential': True,
                        'memory': memory,
                        'cpu': cpu,
                    },
                ]
            )
            print(f"Task definition {task_name} registered.")
            return response['taskDefinition']['taskDefinitionArn']
        except ClientError as e:
            print(f"Error registering task definition: {e}")
            return None

    def create_ecs_service(self, cluster_name, service_name, task_definition_arn, desired_count=1):
        """Create an ECS Service to run the task."""
        try:
            response = self.ecs_client.create_service(
                cluster=cluster_name,
                serviceName=service_name,
                taskDefinition=task_definition_arn,
                desiredCount=desired_count,
                launchType='FARGATE',
                networkConfiguration={
                    'awsvpcConfiguration': {
                        'subnets': ['subnet-xxxxxxxx'],  # Replace with actual subnet ID
                        'assignPublicIp': 'ENABLED'
                    }
                }
            )
            print(f"ECS service {service_name} created.")
            return response['service']['serviceArn']
        except ClientError as e:
            print(f"Error creating ECS service: {e}")
            return None

    def deploy(self, repo_name, image_name, task_name, cluster_name, service_name):
        """Full deploy pipeline: Build Docker image, push to ECR, and deploy to ECS."""
        ecr_repo_uri = self.create_ecr_repository(repo_name)
        if not ecr_repo_uri:
            print("Failed to create or get ECR repository.")
            return

        if not self.build_docker_image(image_name):
            print("Failed to build Docker image.")
            return

        if not self.push_to_ecr(image_name, ecr_repo_uri):
            print("Failed to push Docker image to ECR.")
            return

        task_definition_arn = self.register_task_definition(task_name, f"{ecr_repo_uri}:{image_name}")
        if not task_definition_arn:
            print("Failed to register ECS task definition.")
            return

        service_arn = self.create_ecs_service(cluster_name, service_name, task_definition_arn)
        if service_arn:
            print(f"Deployment successful: {service_arn}")
        else:
            print("Failed to create ECS service.")


# Usage
if __name__ == "__main__":

    load_dotenv()
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
    
    # deployer = ECS(aws_access_key_id=AWS_ACCESS_KEY_ID, 
    #                aws_secret_access_key=AWS_SECRET_KEY)

    # deployer.deploy(
    #     repo_name="datathon2025-pipeline",  # ECR repository name
    #     image_name="datathon2025:preprocess",  # Docker image name
    #     task_name="datathon-preprocess",  # ECS task definition name
    #     cluster_name="datathon2025-cluster",  # ECS cluster name
    #     service_name="datathon-preprocess-service"  # ECS service name
    # )
        