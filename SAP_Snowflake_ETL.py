import pandas as pd
import requests
import snowflake.connector

# Extract from SAP OData
sap_url = 'https://your-sap-server/sap/opu/odata/sap/ZSALES_DATA_SRV/SalesData'
response = requests.get(sap_url, auth=('user', 'pass'))
data = response.json()['d']['results']
df = pd.DataFrame(data)

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='YOUR_USER',
    password='YOUR_PASSWORD',
    account='YOUR_ACCOUNT',
    warehouse='YOUR_WH',
    database='YOUR_DB',
    schema='YOUR_SCHEMA'
)

# Load data to Snowflake
cursor = conn.cursor()
for index, row in df.iterrows():
    cursor.execute("""
        INSERT INTO SAP_SALES_DATA (ID, CUSTOMER, VALUE)
        VALUES (%s, %s, %s)
    """, (row['ID'], row['Customer'], row['SalesValue']))
cursor.close()
conn.close()
