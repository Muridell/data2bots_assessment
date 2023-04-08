###Data2Bots Assessment
#Our data lake is an Amazon s3 bucket:
#Bucket Name: d2b-internal-assessment-bucket
#Data Location: s3://d2b-internal-assessment-bucket/orders_data/*

###The directory contains the following files:
#orders.csv: This data is a fact table about orders gotten on our website
#reviews.csv: This data is a fact table on reviews given for a particular delivered product
#shipment_deliveries.csv: This is a fact table on shipments and their delivery dates

#IMPORT MODULES FOR CONNECTION
import snowflake.connector
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

#CONNECT TO SNOWFLAKE USER ACCOUNT
snowflake_conn_mdata = snowflake.connector.connect(
    user = "MURTADHA_DATA2BOT",
    password = "MURdata2bot",
    account = "zt04410.us-east-2.aws",
    role = "SYSADMIN"
)

#CREATE SNOWFLAKE CURSOR
mcur = snowflake_conn_mdata.cursor()

#CREATE AND USE WAREHOUSE
mcur.execute("""CREATE OR REPLACE WAREHOUSE MUR_COMPUTE_WH""")
mcur.execute("""USE WAREHOUSE MUR_COMPUTE_WH""")

#CREATE AND USE DB
mcur.execute("""CREATE DATABASE MUR_DATA2BOT""")
mcur.execute("""USE DATABASE MUR_DATA2BOT""")

#CREATE AND USE SCHEMA
mcur.execute("""CREATE SCHEMA S3DATA""")
mcur.execute("""USE SCHEMA S3DATA""")

#CREATE STAGING SCHEMA
staging = """CREATE STAGE murtodun9658_staging
url = 's3://d2b-internal-assessment-bucket/orders_data/'
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);"""

mcur.execute(staging)

#CREATE REVIEWS TABLE
mcur.execute("""CREATE OR REPLACE TABLE MUR_DATA2BOT.S3DATA.REVIEWS(
            REVIEW INTEGER NOT NULL,
            PRODUCT_ID INT NOT NULL
            );
""")

#CREATE ORDERS TABLE
mcur.execute("""CREATE OR REPLACE TABLE MUR_DATA2BOT.S3DATA.ORDERS(
            order_id INT NOT NULL,
            customer_id INT NOT NULL,
            order_date DATE NOT NULL,
            product_id VARCHAR NOT NULL,
            unit_price INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            AMOUNT INTEGER NOT NULL
            );
""")

#CREATE SHIPMENT_DELIVERY TABLE
mcur.execute("""CREATE OR REPLACE TABLE MUR_DATA2BOT.S3DATA.SHIPMENT_DELIVERIES(
            SHIPMENT_ID INT NOT NULL,
            ORDER_ID INT NOT NULL,
            SHIPMENT_DATE DATE,
            DELIVERY_DATE DATE
            );
""")

#LOAD REVIEWS.CSV FROM S3 BUCKET INTO REVIEWS TABLE
mcur.execute("""COPY INTO MUR_DATA2BOT.S3DATA.REVIEWS
  FROM @murtodun9658_staging/reviews.csv;
""")

#LOAD ORDERS.CSV FROM S3 BUCKET INTO ORDERS TABLE
mcur.execute("""COPY INTO MUR_DATA2BOT.S3DATA.ORDERS
  FROM @murtodun9658_staging/orders.csv;
""")

#LOAD ORDERS.CSV FROM S3 BUCKET INTO ORDERS TABLE
mcur.execute("""COPY INTO MUR_DATA2BOT.S3DATA.SHIPMENT_DELIVERIES
  FROM @murtodun9658_staging/shipment_deliveries.csv;
""")

##############################CONNECT TO AWS RDS##############################

#CREATE CONNECTION TO DATABASE
conn_rds = psycopg2.connect(
    user = 'murtodun9658',
    password = 'JJuzGOA5e4',
    host = 'd2b-internal-assessment-dwh.cxeuj0ektqdz.eu-central-1.rds.amazonaws.com',
    database = 'd2b_assessment',
    port = 5432,
    )

#CREATE AWS RDS CURSOR
cursor_rds = conn_rds.cursor()

#CREATE DATAFRAMES FROM AWS RDS TABLES
dim_dates = pd.read_sql_query("""SELECT * FROM d2b_assessment.if_common.dim_dates;""", conn_rds)

dim_products = pd.read_sql_query("""SELECT * FROM d2b_assessment.if_common.dim_products;""", conn_rds)

dim_customers = pd.read_sql_query("""SELECT * FROM d2b_assessment.if_common.dim_customers;""", conn_rds)

dim_addresses = pd.read_sql_query("""SELECT * FROM d2b_assessment.if_common.dim_addresses;""", conn_rds)

#CREATE CONNECTION TO SNOWFLAKE USER ACCOUNT THROUGH SNOWFLAKE SQLALCHEMY
engine = create_engine('snowflake://MURTADHA_DATA2BOT:MURdata2bot@zt04410.us-east-2.aws/MUR_DATA2BOT/S3DATA?warehouse=MUR_COMPUTE_WH&role=SYSADMIN')
engine.connect()

#LOAD DATAFRAMES INTO SNOWFLAKE
dim_dates.to_sql('dim_dates', engine, index=False)
dim_products.to_sql('dim_products', engine, index=False) 
dim_customers.to_sql('dim_customers', engine, index=False)
dim_addresses.to_sql('dim_addresses', engine, index=False)

