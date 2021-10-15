import pyodbc as sql
import pandas as pd

#TODO: Clean up code and create functions where necessary
#TODO: Convert in-line queries to stored procedures

def ControlTableValidationCheck(SrcJoinTable, SrcJoinAlias):
    sql_conn = sql.connect('DRIVER={SQL Server Native Client 11.0}; SERVER=SLC-BIDB-S01; DATABASE=DataWarehouse; Trusted_Connection=yes')

    # Creates a cursor from the connection
    cursor = sql_conn.cursor()

    ControlValidation = ('''SELECT concat(DatabaseName,'.' ,SchemaName ,'.' , TableName) as SrcJoinTable,
                                TableAlias
                     FROM [DataWarehouse].[TestResults].[TableNamesControlTableNew]
                           where DatabaseName = \'''' + SrcJoinTable.split('.')[0] + '\'''''
                           and SchemaName = \'''' + SrcJoinTable.split('.')[1] + '\'''''
                           and TableName = \'''' + SrcJoinTable.split('.')[2] + '\'')

    cursor.execute(ControlValidation)
    row = cursor.fetchone()
    TableExists = False
    AliasMatches = False
      #first check is if exists
    if row is not None:
        TableExists = True

        #then check is if alias matches passed in table name
        #row = cursor.fetchone()
        ControlTable = row[0]
        ControlAlias = row[1]

        if SrcJoinTable == ControlTable and SrcJoinAlias == ControlAlias:
            AliasMatches = True

    return [TableExists, AliasMatches]


# Connect to Test Sql Server
sql_conn = sql.connect('DRIVER={SQL Server Native Client 11.0}; SERVER=SLC-BIDB-S01; DATABASE=DataWarehouse; Trusted_Connection=yes')

# Creates a cursor from the connection
cursor = sql_conn.cursor()

# Sql Statements are executed using the Cursor execute function
cursor.execute('''select m.definition
from DataWarehouse.sys.sql_modules       as m
    inner join DataWarehouse.sys.objects as o
        on m.object_id = o.object_id
    inner join DataWarehouse.sys.schemas as s
        on o.schema_id = s.schema_id
where o.type = 'P'
      and o.name not like 'sp_%'
      and m.definition like '%' + 'From' + '%'
      and o.name = 'RFOXTestSrcQuery_FrmControlTbl' ''')

#Returns a single record
row = cursor.fetchone()
#print the result - first element
#print(row[0])

#Remove spaces,newlines, carrriage returns in the query text
#call split() to split the string into a list based on whitespace
qry_txt_list = row[0].split()
#print(qry_txt_list)
#Call join() with " " and list to combine the strings in the list together with a single space
qry_txt = " ".join(qry_txt_list).replace(';','')
#print(qry_txt)

#create a new string with only text following 'from'
from_stmt = qry_txt[qry_txt.find('from'):]
print(from_stmt)

#Split the string following 'from' into a list
from_stmt_list = from_stmt.split()
print(from_stmt_list)
list_to_remove = ['from','as','join','inner','left','outer','=','end','on']
from_stmt_list = [ e for e in from_stmt_list if e not in list_to_remove]
print(from_stmt_list)
df = pd.DataFrame(columns=['SrcJoinTable','SrcJoinAlias','SrcJoinField',
                           'TrgtJoinTable','TrgtJoinAlias','TrgtJoinField',
                          'TableExistsInControl', 'AliasAndTableMatch'])
print(df)

j = int(((len(from_stmt_list) - 2)/4)) + 1
print(j)
counter = 0

for i in range(j):
  SrcJoinTable = ''
  SrcJoinAlias = ''
  SrcJoinField = ''
  TrgtJoinTable = ''
  TrgtJoinAlias = ''
  TrgtJoinField = ''
  
  if i == 0:  
      SrcJoinTable = from_stmt_list[counter]
      counter += 1
      SrcJoinAlias = from_stmt_list[counter]
      counter += 1

      ValidationCheck = ControlTableValidationCheck(SrcJoinTable, SrcJoinAlias)

      df = df.append({'SrcJoinTable' : SrcJoinTable, 'SrcJoinAlias' : SrcJoinAlias, 'SrcJoinField' : '',
                   'TrgtJoinTable' : '', 'TrgtJoinAlias' : '','TrgtJoinField' : '','TableExistsInControl' : ValidationCheck[0],
                   'AliasAndTableMatch' : ValidationCheck[1]},
                    ignore_index = True)
  else:
      SrcJoinTable = from_stmt_list[counter]
      counter += 1
      SrcJoinAlias = from_stmt_list[counter]
      counter += 1
      TrgtJoinField = from_stmt_list[counter]
      TrgtJoinAlias = TrgtJoinField[:TrgtJoinField.find('.')]
      TrgtJoinField = TrgtJoinField[TrgtJoinField.find('.') + 1:]
      counter += 1
      SrcJoinField = from_stmt_list[counter]
      SrcJoinField = SrcJoinField[SrcJoinField.find('.') + 1:]
      counter += 1

      ValidationCheck = ControlTableValidationCheck(SrcJoinTable, SrcJoinAlias)

      cursor.execute('''SELECT concat(DatabaseName,'.' ,SchemaName ,'.' , TableName) as TrgtJoinTable
                     FROM [DataWarehouse].[TestResults].[TableNamesControlTableNew]
                           where TableAlias = \'''' + TrgtJoinAlias + '\'' )

      TrgtJoinTable = cursor.fetchone()[0]

      df = df.append({'SrcJoinTable' : SrcJoinTable, 'SrcJoinAlias' : SrcJoinAlias, 'SrcJoinField' : SrcJoinField,
                     'TrgtJoinTable' : TrgtJoinTable, 'TrgtJoinAlias' : TrgtJoinAlias,'TrgtJoinField' : TrgtJoinField,
                     'TableExistsInControl' : ValidationCheck[0], 'AliasAndTableMatch' : ValidationCheck[1]}, ignore_index = True)

#Close the sql connection
sql_conn.close()

#Enable output window to see all columns in  a dataframe
pd.set_option('display.max_columns', None)
print(df) 

