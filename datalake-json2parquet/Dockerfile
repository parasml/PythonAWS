FROM python:latest

WORKDIR /usr/src/app
RUN python -m pip install \
        git+https://github.com/awslabs/aws-data-wrangler.git@dev \
        boto3 
               
COPY ["config.py","json-to-parquet.py","./"]
CMD ["python", "json-to-parquet.py"]