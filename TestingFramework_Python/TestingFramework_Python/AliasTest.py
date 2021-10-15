import pyodbc as sql
import pandas as pd

sql_conn = sql.connect('DRIVER={SQL Server Native Client 11.0}; SERVER=SLC-BIDB-S01; DATABASE=WarehouseStage; Trusted_Connection=yes')

# Creates a cursor from the connection
cursor = sql_conn.cursor()
cursor.execute('''select
	'WarehouseStage' DatabaseName,
	schema_name(t.schema_id) SchemaName,
	lower(t.name) TableName
from 
	sys.tables t
where 
	schema_name(t.schema_id) = 'rfox'
order by 
	TableName''')

results = cursor.fetchall()
sql_conn.close()

#we need to generate the shortest possible unique value for each table name
for i in results:
	print(i[2])

