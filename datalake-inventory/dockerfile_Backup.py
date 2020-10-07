FROM python:latest
 
RUN apt-get update && apt-get install -y libpq-dev gcc

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

RUN pip install --trusted-host pypi.python.org pyodbc==4.0.26
RUN pip3 install psycopg2~=2.6
RUN apt-get autoremove -y gcc
WORKDIR /usr/src/app
RUN python -m pip install \
                    boto3 \
                    psycopg2\
                    awswrangler

COPY ["main.py", "config.py", "ECS_handler.py", "postgresDB.py", "splitPaqruetFile.py", "LogError.py", "./"]
CMD [ "python", "main.py" ]