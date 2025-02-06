# BIA-Datathon-2025
<b>This repository contains the project for the BIA Datathon 2025. The main objective is to create a serverless end-to-end solution architecture that will do the following:</b>
1. Automate data ingestion through 3 ways - Scheduled web scraping, Extracting data from internal pdf files using OCR, and manually calling the API to upload data.
2. Daily schedule for lambda function to check for changes in S3 bucket, which will then trigger the pipeline to run.
3. Pipeline Component 1 - Pre-process the text data to feed into the pre-trained model.
4. Pipeline Component 2 - Validate the data to ensure XXX
5. Pipeline Component 3 - Extract Entity-Relationship using specified pre-trained model, saving the output to S3.
6. Pipeline Component 4 - From S3, upload the output to Neo4j Managed Graph Database AuraDB, for further insight explorations using Neo4j suite of visualisation tools.
<br>
<b>Files Overview:</b>

```
📦 Project Root
├── 📂 Dockerfiles               # Contains Dockerfiles for different components
│   ├── Dockerfile.model         # Dockerfile for model container
│   ├── Dockerfile.neo4j         # Dockerfile for Neo4j container
│   ├── Dockerfile.preprocess    # Dockerfile for data preprocessing
│   ├── Dockerfile.runpipeline   # Dockerfile for lambda function to run the pipeline
│   ├── Dockerfile.validation    # Dockerfile for validation (empty)
│
├── 📂 functions                 # Utility functions for various components
│   ├── __init__.py              # Package initializer
│   ├── aws_utils.py             # AWS-related utility functions
│   ├── neo4j_utils.py           # Neo4j-related utility functions
│   ├── rebel_utils.py           # Utility functions for Rebel (model) component
│   ├── relik_utils.py           # Utility functions for Relik (model) component
│   ├── utils.py                 # General utility functions
│
├── 📂 requirements              # Dependencies for each module
│   ├── model.txt                # Dependencies for models
│   ├── neo4j.txt                # Dependencies for uploading files to Neo4j
│   ├── preprocess.txt           # Dependencies for preprocessing
│   ├── runpipeline.txt          # Dependencies for lambda function to run the pipeline
│   ├── validation.txt           # Dependencies for validation (empty)
│
├── data_preprocessing.py        # Script for data preprocessing
├── main.py                      # Main script for creating the pipeline
├── model.py                     # Script to run model to extract entity-relationship
├── run_pipeline.py              # Script for lambda function to excute the pipeline given condition
├── upload_to_neo4j.py           # Script to upload data to Neo4j
├── requirements.txt             # Dependencies for running the main script
├── README.md                    # Project documentation
```

<br>
<b>To replicate this project:</b>
1. Clone the repository.
2. Install dependencies via `pip install -r requirements.txt`.
3. Run the `main.py` script, which will do the following:
  -  Creates the images required for pipeline to work
  -  Initializes the step function
  -  Creates the images for the lambda functions, including the one that triggers the pipeline daily.
4. Set up Amazon EventBridge Schedule and set target to the lambda function image created in previous step.
<br>
As the components in this pipeline are modular, to makes any changes you may make edits to the json text in XXX.
<br>
<b>To utilise models not included in this repository:</b>
- Ensure that there are NO conflict in dependencies.***
- Create a `<model>_utils.py` to include all the relevant functions
- Adhere to the DataFrame format (`entities_df`, `relationships_df`) and output format (.csv)
- Include the initialization of the model in `model.py`
- Edit the `'-m'` parameter in `Dockerfile.model` to the model of your choice
