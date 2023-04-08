#EXPORTING DATA FROM SNOWFLAKE TO DATA LAKE AMAZON S3

#IMPORT SNOWFLAKE CONNECTION MODULE
import snowflake.connector

#CONNECT TO SNOWFLAKE USER ACCOUNT
snowflake_conn_mdata = snowflake.connector.connect(
    user = "MURTADHA_DATA2BOT",
    password = "MURdata2bot",
    account = "zt04410.us-east-2.aws",
    role = "SYSADMIN"
)

#CREATE SNOWFLAKE CURSOR
mcur = snowflake_conn_mdata.cursor()

# WRITE QUERY TO LOAD LATE_SHIPMENTS to S3
export_late_shipments = """
copy into @murtodun9658_staging/analytics_export/murtodun9658/late_shipments.csv
from (SELECT * FROM ANALYTICS.MURTODUN9658_ANALYTICS.LATE_SHIPMENTS)
single=TRUE
header=TRUE
OVERWRITE=TRUE
file_format = (type = CSV COMPRESSION = NONE )
"""

# WRITE QUERY TO LOAD UNDELIVERED_SHIPMENTS to S3
export_undelivered_shipments = """
copy into @murtodun9658_staging/analytics_export/murtodun9658/undelivered_shipments.csv
from (SELECT * FROM ANALYTICS.MURTODUN9658_ANALYTICS.UNDELIVERED_SHIPMENTS)
single=TRUE
header=TRUE
OVERWRITE=TRUE
file_format = (type = CSV COMPRESSION = NONE )
"""

# WRITE QUERY TO LOAD AGG_PUBLIC_HOLIDAYS to S3
export_agg_holiday = """
copy into @murtodun9658_staging/analytics_export/murtodun9658/agg_public_holiday.csv
from (SELECT * FROM ANALYTICS.MURTODUN9658_ANALYTICS.AGG_PUBLIC_HOLIDAY)
single=TRUE
header=TRUE
OVERWRITE=TRUE
file_format = (type = CSV COMPRESSION = NONE )
"""

# WRITE QUERY TO LOAD AGG_SHIPMENTS to S3
export_agg_shipments = """
copy into @murtodun9658_staging/analytics_export/murtodun9658/agg_shipments.csv
from (SELECT * FROM ANALYTICS.MURTODUN9658_ANALYTICS.AGG_SHIPMENTS)
single=TRUE
header=TRUE
OVERWRITE=TRUE
file_format = (type = CSV COMPRESSION = NONE )
"""

# WRITE QUERY TO LOAD BEST_PERFORMING_PRODUCT TO S3
export_best_product = """
copy into @murtodun9658_staging/analytics_export/murtodun9658/best_performing_product.csv
from (SELECT * FROM ANALYTICS.MURTODUN9658_ANALYTICS.BEST_PERFORMING_PRODUCT)
single=TRUE
header=TRUE
OVERWRITE=TRUE
file_format = (type = CSV COMPRESSION = NONE )
"""
 

#SPECIFY ROLE AND WAREHOUSE TO USE
mcur.execute("""USE ROLE TRANSFORMER""")
mcur.execute("""USE WAREHOUSE COMPUTE_WH""")


#RUN SQL QUERIES TO LOAD TABLES TO S3
mcur.execute(export_late_shipments)
mcur.execute(export_undelivered_shipments)
mcur.execute(export_agg_holiday)
mcur.execute(export_agg_shipments)
mcur.execute(export_best_product)
