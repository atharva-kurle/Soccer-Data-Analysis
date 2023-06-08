import redshift_connector


# Credentials
AWS_ACCESS_KEY_ID = "<YOUR_ACCESS_KEY>"
AWS_SECRET_ACCESS_KEY = "YOUR_SECRET_ACCESS_KEY"
AWS_REGION = "ap-south-1"
SCHEMA_NAME = "<ATHENA_DATABASE_NAME>"
S3_STAGING_DIR = "<ATHENA_QUERY_OUTPUT_BUCKET>"
S3_BUCKET_NAME = "<SOURCE_BUCKET_NAME>"
S3_OUTPUT_DIR = "<OUTPUT_DIR>"
CLEANED_BUCKET = '<CLEANED DATA BUCKET>'


conn = redshift_connector.connect(
    host='<REDSHIFT_HOST>',
    database='<DATABASE_NAME>',
    user='<USER>',
    password='<PASSWORD>',
    port=5439
 )

conn.autocommit = True

cursor = redshift_connector.Cursor = conn.cursor()

cursor.execute("""
copy clubDim from 's3://<CLEANED_BUCKET_NAME>/output/clubDim.csv'
credentials '<ROLE_ASSOCIATED_FOR_REDSHIFT>'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")

cursor.execute("""
copy teamDim from 's3://<CLEANED_BUCKET_NAME>/output/teamDim.csv'
credentials '<ROLE_ASSOCIATED_FOR_REDSHIFT>'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")

cursor.execute("""
copy matchFact from 's3://<CLEANED_BUCKET_NAME>/output/matchFact.csv'
credentials '<ROLE_ASSOCIATED_FOR_REDSHIFT>'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")


cursor.execute("""
copy stadiumDim from 's3://<CLEANED_BUCKET_NAME>/output/stadiumDim.csv'
credentials '<ROLE_ASSOCIATED_FOR_REDSHIFT>'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")


cursor.execute("""
copy refDim from 's3://<CLEANED_BUCKET_NAME>/output/refDim.csv'
credentials '<ROLE_ASSOCIATED_FOR_REDSHIFT>'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")

cursor.execute("""
copy countryDim from 's3://<CLEANED_BUCKET_NAME>/output/countryDim.csv'
credentials '<ROLE_ASSOCIATED_FOR_REDSHIFT>'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")


cursor.execute("""
copy yearDim from 's3://<CLEANED_BUCKET_NAME>/output/yearDim.csv'
credentials '<ROLE_ASSOCIATED_FOR_REDSHIFT>'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")

cursor.execute("""
copy posDim from 's3://<CLEANED_BUCKET_NAME>/output/posDim.csv'
credentials '<ROLE_ASSOCIATED_FOR_REDSHIFT>'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")

conn.close()