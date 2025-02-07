from functions.aws_utils import *

from dotenv import load_dotenv
import os       
import json

if __name__ == "__main__":

    load_dotenv()
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

    s3 = S3(aws_access_key_id=AWS_ACCESS_KEY_ID, 
            aws_secret_access_key=AWS_SECRET_KEY)
    
    response = s3.read_from_s3("datathon2025",
                    "configs/sf_vars.json",
                    df=False)
    
    sf_vars = json.loads(response.read().decode('utf-8'))

    sf = StepFunction(aws_access_key_id=AWS_ACCESS_KEY_ID, 
                   aws_secret_access_key=AWS_SECRET_KEY)

    sf.start_step_function_execution(sf_vars['state_machine_arn'])
