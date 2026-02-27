import snowflake.connector

conn = snowflake.connector.connect(
    user='******',
    password='*******',
    account='******',
    warehouse='ETL_WH',
    database='ETL_DB',
    schema='PUBLIC',
    role= "ACCOUNTADMIN",
)

cursor = conn.cursor()

cursor.execute("SELECT * FROM customers")

rows = cursor.fetchall()

for row in rows:
    print(row)

cursor.close()
conn.close()
