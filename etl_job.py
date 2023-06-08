import boto3
import pandas as pd
import time
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


# Connecting to Athena
athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

attendance_query_exec = athena_client.start_query_execution(
    QueryString='SELECT * FROM attendance',
    QueryExecutionContext={
        'Database': SCHEMA_NAME
    },
    ResultConfiguration={
        'OutputLocation': S3_STAGING_DIR
    }
)


goals_query_exec = athena_client.start_query_execution(
    QueryString='SELECT * FROM goals',
    QueryExecutionContext={
        'Database': SCHEMA_NAME
    },
    ResultConfiguration={
        'OutputLocation': S3_STAGING_DIR
    }
)


team_query_exec = athena_client.start_query_execution(
    QueryString='SELECT * FROM team',
    QueryExecutionContext={
        'Database': SCHEMA_NAME
    },
    ResultConfiguration={
        'OutputLocation': S3_STAGING_DIR
    }
)
time.sleep(3)

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

s3_client.download_file(S3_BUCKET_NAME,S3_OUTPUT_DIR+"/"+attendance_query_exec['QueryExecutionId'] + ".csv",attendance_query_exec['QueryExecutionId']+'.csv')

s3_client.download_file(S3_BUCKET_NAME,S3_OUTPUT_DIR+"/"+goals_query_exec['QueryExecutionId'] + ".csv",goals_query_exec['QueryExecutionId']+'.csv')

s3_client.download_file(S3_BUCKET_NAME,S3_OUTPUT_DIR+"/"+team_query_exec['QueryExecutionId'] + ".csv",team_query_exec['QueryExecutionId']+'.csv')

attendenceDF = pd.read_csv(attendance_query_exec['QueryExecutionId']+".csv")
goalsDF = pd.read_csv(goals_query_exec['QueryExecutionId']+".csv")
teamDF = pd.read_csv(team_query_exec['QueryExecutionId']+".csv")

teamDF.drop(['no','caps','dob/age'],axis=1,inplace=True)
attendenceDF.drop(['stage','url'],axis=1,inplace=True)


# # position dim
position_dict = {"pos":teamDF['pos'].unique()}
posDim = pd.DataFrame(position_dict)
posDim.index += 1
posDim['pos_id'] = posDim.index

# # team dim
team_dict = {"teams":teamDF['country'].unique()}
teamDim = pd.DataFrame(team_dict)
teamDim.index += 1

# # club dim
club_dict = teamDF[['club','clubcountry']]
clubDim = club_dict.drop_duplicates(subset=['club'])
clubDim.reset_index(drop=True,inplace=True)
clubDim.index += 1
clubDim['club_id'] = clubDim.index

# # year dim
year_dict = {"year":teamDF['year'].unique()}
yearDim = pd.DataFrame(year_dict).dropna()
yearDim.index += 1
yearDim['year_id'] = yearDim.index

# #country dim
country_dict = {"country":teamDF['country'].unique()}
countryDim = pd.DataFrame(country_dict).dropna()
countryDim.index += 1
countryDim['country_id'] = countryDim.index

# # referee dim
referee_dict = {"referee":attendenceDF['referee'].unique()}
refDim = pd.DataFrame(referee_dict)
refDim.index += 1
refDim['ref_id'] = refDim.index

# #stadium dim
stadium_dict = {"stadium":attendenceDF['stadium'].unique()}
stadiumDim = pd.DataFrame(stadium_dict)
stadiumDim.index += 1
stadiumDim['stadium_id'] = stadiumDim.index


teamDim1 = teamDF.merge(posDim, on='pos', how='left').drop('pos',axis=1)
teamDim2 = teamDim1.merge(clubDim, on='club', how='left').drop(['club','clubcountry_x','clubcountry_y'],axis=1)
teamDim = teamDim2.merge(yearDim, on='year', how='left').drop('year',axis=1)

matchFact1 = attendenceDF.merge(refDim, on='referee', how='left').drop(['pk','referee'],axis=1)
matchFact2 = matchFact1.merge(stadiumDim, on='stadium', how='left').drop('stadium',axis=1)
matchFact3 = matchFact2.merge(countryDim, left_on='team1', right_on='country', how='left').drop('team1',axis=1)
matchFact4 = matchFact3.merge(countryDim, left_on='team2', right_on='country', how='left').drop(['team2','country_y','country_x'],axis=1)
matchFact5 = matchFact4.merge(goalsDF, on='game_id', how='left')
matchFact6 = matchFact5.merge(teamDF, on='player', how='left').drop(['player','pos','club','country','clubcountry','year'],axis=1)
matchFact = matchFact6.dropna()

time.sleep(1)
matchFact.to_csv('<YOUR_LOCAL_PATH>\\matchFact.csv',index=False)
teamDim.to_csv('<YOUR_LOCAL_PATH>\\teamDim.csv',index=False)
stadiumDim.to_csv('<YOUR_LOCAL_PATH>\\stadiumDim.csv',index=False)
refDim.to_csv('<YOUR_LOCAL_PATH>\\refDim.csv',index=False)
countryDim.to_csv('<YOUR_LOCAL_PATH>\\countryDim.csv',index=False)
yearDim.to_csv('<YOUR_LOCAL_PATH>\\yearDim.csv',index=False)
clubDim.to_csv('<YOUR_LOCAL_PATH>\\clubDim.csv',index=False)
posDim.to_csv('<YOUR_LOCAL_PATH>\\posDim.csv',index=False)

time.sleep(1)
s3_client.upload_file('<YOUR_LOCAL_PATH>\\matchFact.csv', CLEANED_BUCKET, S3_OUTPUT_DIR+'/matchFact.csv')
s3_client.upload_file('<YOUR_LOCAL_PATH>\\teamDim.csv', CLEANED_BUCKET, S3_OUTPUT_DIR+'/teamDim.csv')
s3_client.upload_file('<YOUR_LOCAL_PATH>\\stadiumDim.csv', CLEANED_BUCKET, S3_OUTPUT_DIR+'/stadiumDim.csv')
s3_client.upload_file('<YOUR_LOCAL_PATH>\\refDim.csv', CLEANED_BUCKET, S3_OUTPUT_DIR+'/refDim.csv')
s3_client.upload_file('<YOUR_LOCAL_PATH>\\countryDim.csv', CLEANED_BUCKET, S3_OUTPUT_DIR+'/countryDim.csv')
s3_client.upload_file('<YOUR_LOCAL_PATH>\\yearDim.csv', CLEANED_BUCKET, S3_OUTPUT_DIR+'/yearDim.csv')
s3_client.upload_file('<YOUR_LOCAL_PATH>\\clubDim.csv', CLEANED_BUCKET, S3_OUTPUT_DIR+'/clubDim.csv')
s3_client.upload_file('<YOUR_LOCAL_PATH>\\posDim.csv', CLEANED_BUCKET, S3_OUTPUT_DIR+'/posDim.csv')

teamDimSchema = pd.io.sql.get_schema(teamDim,'teamDim')
matchFactSchema = pd.io.sql.get_schema(matchFact,'matchFact')
stadiumDimSchema = pd.io.sql.get_schema(stadiumDim,'stadiumDim')
refDimSchema = pd.io.sql.get_schema(refDim,'refDim')
countryDimSchema = pd.io.sql.get_schema(countryDim,'countryDim')
yearDimSchema = pd.io.sql.get_schema(yearDim,'yearDim')
clubDimSchema = pd.io.sql.get_schema(clubDim,'clubDim')
posDimSchema = pd.io.sql.get_schema(posDim,'posDim')


conn = redshift_connector.connect(
    host='<REDSHIFT_HOST>',
    database='<DATABASE_NAME>',
    user='<USER>',
    password='<PASSWORD>',
    port=5439
 )

conn.autocommit = True

cursor = redshift_connector.Cursor = conn.cursor()
cursor.execute(teamDimSchema)
cursor.execute(matchFactSchema)
cursor.execute(stadiumDimSchema)
cursor.execute(refDimSchema)
cursor.execute(countryDimSchema)
cursor.execute(yearDimSchema)
cursor.execute(clubDimSchema)
cursor.execute(posDimSchema)


conn.close()