FROM python:latest

WORKDIR /usr/src/app


# install FreeTDS and dependencies
RUN apt-get update \
 && apt-get install unixodbc -y \
 && apt-get install unixodbc-dev -y \
 && apt-get install freetds-dev -y \
 && apt-get install freetds-bin -y \
 && apt-get install tdsodbc -y \
 && apt-get install --reinstall build-essential -y

 # populate "ocbcinst.ini"
RUN echo "[FreeTDS]\n\
Description = FreeTDS unixODBC Driver\n\
Driver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\n\
Setup = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so" >> /etc/odbcinst.ini

# install pyodbc
RUN pip install --trusted-host pypi.python.org pyodbc==4.0.26
#pip install git+https://github.com/awslabs/aws-data-wrangler.git@dev
RUN python -m pip install \
        awswrangler \
        boto3 
        
               
COPY ["config.py","datalake-dms-reconcile-recordcount.py","./"]
CMD ["python", "datalake-dms-reconcile-recordcount.py"]