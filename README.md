Welcome to Murtadha Oduntan's Data2bots Assessments README file!

This is for the technical assessment phase associated with the Analytics Engineering role at Data2Bots.

### Data Sources:
- AWS S3: Data Lake
- Amazon RDS (PostgreSQL)

### Data Warehouse:
- Snowflake

### ETL Tools
- Extract and Load data: Python
- Data Transformation: SQL

### Data Transformation and Modelling:
- dbt (Data Build Tool)

### Data Visualisation and Reporting (BI Tool):
- Preset Cloud

Access to the credentials for the assessment were provided using the link below:
https://us-central1-d2b-sdbx.cloudfunctions.net/d2b_assessment_credentials_generator

First Name: Murtadha
Last Name: Oduntan

I created a 30-days free trial account on Snowflake to be used as the Data Warehouse.

After setting up the warehouse, I created a user, MURTADHA_DATA2BOT with SYSADMIN role.

TO extract data from the data sources and load into the warehouse, I created connections to the sources then pulled data strategically for each source, to be loaded into the warehouse. These methods are:
- For AWS S3 source: Created a staging schema on Snowflake in order to load the source files from the S3 bucket into the multiple tables created in Snowflake.
- For Amazon RDS source: I created Pandas dataframes from the dim_ tables in the *if_common* schema then used the to_sql() function of the dataframe to load the corresponding data into snowflake by using the Snowflake SQLAlchemy module to create a connection engine. 
Here's the Snowflake documentation link: https://docs.snowflake.com/en/developer-guide/python-connector/sqlalchemy

After successfully loading the data into the tables on Snowflake, I then created a warehouse and role which I subsequently used when setting up the dbt Project for data transformation and modelling. While setting up the project on dbt, I created connection to the data warehouse on Snowflake and set the database and schema where the transformed data and models would reside.

I then extracted the required insights from the data. This included agg_public_holiday, late_shipments, undelivered_shipments, agg_shipments and best_performing_product as stated in the Assessment Guide provided by the Data2bots team.
I also created product_reviews and order_shipments models which I subsequently used in creating a visualisation dashboard to present my findings from the data.

I have uploaded the image of the dashboard data-2-bots-chambua-inc-dashboard.jpg in the assessment repo.
You can view and interact with the dashboard via the link:
https://34a66c17.us1a.app.preset.io/superset/dashboard/p/r4kxBDXBe2P/

Preset Cloud Login Details:
- username: murtadhadata2bot@gmail.com
- password: MURdata2bots!

Thank you very much 🙂
