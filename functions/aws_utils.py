import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import botocore
from dotenv import load_dotenv
import subprocess
import os
from io import StringIO
import pandas as pd
import re
import datetime
from pytz import timezone


# Connection class
class AWSConnection:
    def __init__(self, aws_access_key_id, aws_secret_key, region_name='ap-southeast-1'):

        try:
            self.session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
            )
            print("Connection to AWS Established!")
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"Error: {e}")
            self.session = None

        self.region_name = region_name

    # Retrieve client for particular service E.g. S3, ECS, EC2
    def get_client(self, client_name):
        if self.session:
            print(f"{client_name.upper()} session successfully initialized.")
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
    def upload_to_s3(self, bucket_name, prefix, file_name, file, df=True):

        # file_path e.g. clean_data/

        if df:
            # Create an in-memory buffer
            csv_buffer = StringIO()

            # Write the df to the buffer
            file.to_csv(csv_buffer, index=False)
            file = csv_buffer.getvalue()

        s3_file_path = f"{prefix}/{file_name}"

        try:
            # Uploading file
            self.s3.put_object(Bucket=bucket_name, Key=s3_file_path, Body=file)
            print(f"file '{file_name}' has been uploaded to S3 at '{s3_file_path}'")

        except Exception as e:
            print(f"Error uploading file to S3: {e}")



    # Read file from S3
    def read_from_s3(self, bucket_name, file_path, df=True):
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=file_path)
            # Read the CSV file directly from S3 into a DataFrame

            output = response['Body']
            if df:
                output = pd.read_csv(output)  # 'Body' contains the file content
            
            print(f"file from {file_path} successfully loaded.")
            return output
        
        except Exception as e:
            print(f"Error reading file from S3: {e}")
            return None
        

    def check_file_update(self, bucket_name, file_path):
        sg_timezone = timezone("Asia/Singapore")

        response = self.s3.head_object(Bucket=bucket_name, Key=file_path)
        last_modified = response['LastModified'].astimezone(sg_timezone)

        # Convert to UTC date for comparison
        last_modified_date = last_modified.date()
        today = datetime.datetime.now(sg_timezone).date()

        if last_modified_date == today:
            print(f"✅ File '{bucket_name}' was updated today ({last_modified_date}).")
            return True
        else:
            print(f"❌ File '{bucket_name}' was last modified on {last_modified_date}, no new updates.")
            return False


class ECS:
    def __init__(self, aws_access_key_id, aws_secret_access_key):
        aws_connection = AWSConnection(aws_access_key_id, aws_secret_access_key)
        self.ecs_client = aws_connection.get_client("ecs")
        self.ecr_client = aws_connection.get_client("ecr")
        self.region = aws_connection.region_name
    
    def get_ecr_login_password(self):
        """Retrieve ECR login password using boto3."""
        response = self.ecr_client.get_authorization_token()
        auth_data = response['authorizationData'][0]
        login_password = auth_data['authorizationToken']

        return login_password
    
    def create_ecr_repository(self, repo_name):
        """Create an ECR repository if it doesn't exist."""
        try:
            # Check if the repository already exists
            response = self.ecr_client.describe_repositories(repositoryNames=[repo_name])
            repo_uri = response["repositories"][0]["repositoryUri"]
            print(f"Repository {repo_name} already exists: {repo_uri}")
            return repo_uri
        
        except ClientError as e:
            if e.response["Error"]["Code"] == "RepositoryNotFoundException":
                try:
                    # Create the repository if it doesn't exist
                    response = self.ecr_client.create_repository(repositoryName=repo_name)
                    repo_uri = response["repository"]["repositoryUri"]
                    print(f"Repository {repo_name} created: {repo_uri}")
                    return repo_uri
                
                except ClientError as create_error:
                    print(f"Error creating repository: {create_error}")
                    return None
            else:
                print(f"Error checking repository: {e}")
                return None
    
    def build_docker_image(self, image_name, dockerfile_path):
        """Build the Docker image using the Dockerfile."""
        print(f"Building Docker image: {image_name}")
        try:
            subprocess.check_call(['docker', 'build', '-t', image_name, '-f', dockerfile_path, '.'])
        except subprocess.CalledProcessError as e:
            print(f"Error building Docker image: {e}")
            return False
        return True
    
    def push_to_ecr(self, image_name, ecr_repo_uri):
        """Tag the Docker image and push it to ECR."""
        try:
            # Login to ECR
            print("Logging into ECR...")

            login_password = self.get_ecr_login_password()

            subprocess.run(
                ['docker', 'login', '--username', 'AWS', '--password-stdin', ecr_repo_uri],
                input=login_password.encode("utf-8")
            )

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
    
    def register_task_definition(self, task_name, image_uri, container_name='container'):
        """Register a new ECS Task Definition."""
        try:
            response = self.ecs_client.register_task_definition(
                family=task_name,
                cpu='256',
                memory='512',
                networkMode='awsvpc',
                requiresCompatibilities = ["FARGATE"],
                executionRoleArn='hackathon2025-task-executor',
                containerDefinitions=[
                    {
                        'name': container_name,
                        'image': image_uri,
                        'essential': True,
                        'memory': 512,
                        'cpu': 256,
                        'essential': True,
                        'logConfiguration': {
                        'logDriver': 'awslogs',
                        'options': {
                        'awslogs-group': '/ecs/datathon2025-log-group',
                        'awslogs-region': self.region,
                        'awslogs-stream-prefix': 'ecs'
                        }}
                    },
                ]
            )
            print(f"Task definition {task_name} registered.")
            return response['taskDefinition']['taskDefinitionArn']
        except ClientError as e:
            print(f"Error registering task definition: {e}")
            return None
        
    def deploy(self, repo_name, image_name, task_name, cluster_name, dockerfile_path):
        """Full deploy pipeline: Build Docker image, push to ECR, and deploy to ECS."""
        ecr_repo_uri = self.create_ecr_repository(repo_name)
        if not ecr_repo_uri:
            print("Failed to create or get ECR repository.")
            return

        if not self.build_docker_image(image_name, dockerfile_path):
            print("Failed to build Docker image.")
            return

        print("ECR_REPO_URI: ", ecr_repo_uri)

        if not self.push_to_ecr(image_name, ecr_repo_uri):
            print("Failed to push Docker image to ECR.")
            return

        task_definition_arn = self.register_task_definition(task_name, f"{ecr_repo_uri}:{image_name}")

        return task_definition_arn
        if not task_definition_arn:
            print("Failed to register ECS task definition.")
            return


class StepFunction:
    def __init__(self, aws_access_key_id, aws_secret_access_key):
        aws_connection = AWSConnection(aws_access_key_id, aws_secret_access_key)
        self.sf_client = aws_connection.get_client("stepfunctions")
        self.region = aws_connection.region_name    

    def create_step_function(self, state_machine_name, state_machine_definition, role_arn):
        # Create the state machine
        try:
            response = self.sf_client.create_state_machine(
                name=state_machine_name,
                definition=state_machine_definition,
                roleArn=role_arn
            )
            
            return response['stateMachineArn']
        
        except Exception as e:
            if 'StateMachineAlreadyExists' in str(e): 
                
                arn_match = re.search(r"State Machine Already Exists: '([^']+)'", str(e))
                if arn_match:
                    state_machine_arn = arn_match.group(1)

                self.sf_client.update_state_machine(
                stateMachineArn=state_machine_arn,
                # stateMachineArn="arn:aws:states:ap-southeast-1:484907528704:stateMachine:Datathon2025StateMachine",
                definition=state_machine_definition
                )

                return state_machine_arn
    
    def start_step_function_execution(self, state_machine_arn, input_data={}):
        """Start a new Step Functions execution."""
        try:
            response = self.sf_client.start_execution(
                stateMachineArn=state_machine_arn,
                input=str(input_data)  # Pass any input you want to provide to the state machine
            )
            print(f"Started Step Functions execution with ARN: {response['executionArn']}")
            return response['executionArn']
        
        except Exception as e:
            print(f"Error starting Step Functions execution: {e}")
            return None
        
    def check_execution_status(self, execution_arn):
        """Check the status of a running Step Functions execution."""
        try:
            response = self.sf_client.describe_execution(
                executionArn=execution_arn
            )
            status = response['status']
            print(f"Execution status: {status}")
            return status
            
        except Exception as e:
            print(f"Error getting execution status: {e}")
            return None
        