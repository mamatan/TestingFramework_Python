import os
import numpy as np
import pandas as pd
import pyodbc as sql

#Import the csv file into pandas df

df = pd.read_csv ('C:/Users/mnayak/OneDrive - CHG Healthcare/Desktop/GitHubPersonal/INT072_Candidates_202209021232.csv').fillna('null')
#print(df)

# Clean table names: 
#all lower case 
# remove white spaces with _  
# replace $,-,/,// with _

file = 'INT072_Candidates_202209021232.csv'
clean_tbl_name = file.lower().replace(' ', '_').replace('?','') \
                      .replace('-','_').replace(r'/','_').replace('\\','_').replace('%','') \
                      .replace(')','').replace(r'(','').replace('$','')
tbl_name = format(clean_tbl_name.split('.') [0])
schema_name = 'TestResults.'
print(tbl_name)

# Clean column names:
#all lower case 
# remove white spaces with _  
# replace $,-,/,// with _

df.columns = [x.lower().replace(' ', '_').replace('?','') \
                      .replace('-','_').replace(r'/','_').replace('\\','_').replace('%','') \
                      .replace(')','').replace(r'(','').replace('$','') for x in df.columns]

# print(df.columns) 
# print(df.dtypes)  
# replace df data types to match with sql data types
replacements = {
                'object' : 'nvarchar',
                'float64' : 'float',
                'int64' : 'int',
                'datetime64' : 'datetime'
                }
#print(replacements)       
# create a column string with column name and datatype which should be sql data type inorder to automate create sql statement
col_str = ',' .join('{} {}'.format(n, d) for (n, d) in zip(df.columns,df.dtypes.replace(replacements)))  
print(col_str)

# Open a database coonection
sql_conn = sql.connect('DRIVER={SQL Server Native Client 11.0}; SERVER=SLC-BIDB-S01; DATABASE=DataWarehouse; Trusted_Connection=yes')
cursor = sql_conn.cursor()
print('opened db successfully')

#Drop tables with same name
cursor.execute('Drop table if exists %s %s;' %(schema_name,tbl_name))

#Create table
cursor.execute('Create Table %s %s (%s)' %(schema_name,tbl_name,col_str)) 
print('table was created successfully'.format(tbl_name))
sql_conn.commit()
sql_conn.close()


#Insert values to table


