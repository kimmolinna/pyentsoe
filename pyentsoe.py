from entsoe import EntsoePandasClient
import datetime as dt
import pandas as pd
import awswrangler as wr
# import boto3
import keyring
import sys

if len(sys.argv) < 2:
    year = dt.datetime.now().year
else:
    year = int(sys.argv[1])

client = EntsoePandasClient(api_key = str(keyring.get_password('entsoe','kimmo')))

start = pd.Timestamp(str(year)+"0101", tz='Europe/Helsinki')
end = pd.Timestamp(str(year+1)+"0101",tz='Europe/Helsinki')
country_code = 'FI'

ts=client.query_day_ahead_prices(country_code, start=start, end=end)
df=pd.DataFrame({"timestamp":ts.index, "price": ts.values})
# Cloudflare
# b3_session = boto3.Session(profile_name="cloudflare")
# wr.config.s3_endpoint_url = 'https://' + str(keyring.get_password('r2','account_id')) + '.r2.cloudflarestorage.com'
# wr.s3.to_parquet(df, "s3://linna/entsoe/entsoe_"+ str(year) +".parquet", boto3_session=b3_session)

# AWS
wr.s3.to_parquet(df, "s3://linna/entsoe/entsoe_"+ str(year) +".parquet")