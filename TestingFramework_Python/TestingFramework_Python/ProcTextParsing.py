import pyodbc as sql
import pandas as pd

# TODO: Possible Solutions: Come up with a control table for datawarehouse objects that links to all dimensionalID's


def control_table_validation_check(src_join_table, src_join_alias):
    sql_conn = sql.connect(
        'DRIVER={SQL Server Native Client 11.0}; SERVER=SLC-BIDB-S01; DATABASE=DataWarehouse; Trusted_Connection=yes')

    # Creates a cursor from the connection
    cursor = sql_conn.cursor()

    # Execute the stored proc with parameters
    control_validation = ("EXEC [TestResults].[LkpTblName_Alias] @DBName = '" + src_join_table.split('.')[0] +
                          "' ,@SchemaName = '" + src_join_table.split('.')[1] + "', @TableName = '" + src_join_table.split('.')[2] + "'")

    cursor.execute(control_validation)
    row = cursor.fetchone()
    table_exists = False
    alias_matches = False
    # first check is if exists
    if row is not None:
        table_exists = True

        # then check is if alias matches passed in table name
        #row = cursor.fetchone()
        control_table = row[0]
        control_alias = row[1]

        if src_join_table == control_table and src_join_alias == control_alias:
            alias_matches = True

    return [table_exists, alias_matches]


# Connect to Test Sql Server
sql_conn = sql.connect(
    'DRIVER={SQL Server Native Client 11.0}; SERVER=SLC-BIDB-S01; DATABASE=DataWarehouse; Trusted_Connection=yes')

# Creates a cursor from the connection
cursor = sql_conn.cursor()

# Sql Statements are executed using the Cursor execute function
cursor.execute("EXEC [TestResults].[ExecSP_RFOXTestSrcQuery_FrmControlTbl]")

# Returns a single record
row = cursor.fetchone()
# print the result - first element
# print(row[0])

# Remove spaces,newlines, carrriage returns in the query text
# call split() to split the string into a list based on whitespace
qry_txt_list = row[0].split()
# print(qry_txt_list)
# Call join() with " " and list to combine the strings in the list together with a single space
qry_txt = " ".join(qry_txt_list).replace(';', '')
# print(qry_txt)

# create a new string with only text following 'from'
from_stmt = qry_txt[qry_txt.find('from'):]
print(from_stmt)

# Split the string following 'from' into a list
from_stmt_list = from_stmt.split()
print(from_stmt_list)
list_to_remove = ['from', 'as', 'join',
                  'inner', 'left', 'outer', '=', 'end', 'on']
from_stmt_list = [e for e in from_stmt_list if e not in list_to_remove]
print(from_stmt_list)
df = pd.DataFrame(columns=['SrcJoinTable', 'SrcJoinAlias', 'SrcJoinField',
                           'TrgtJoinTable', 'TrgtJoinAlias', 'TrgtJoinField',
                           'TableExistsInControl', 'AliasAndTableMatch'])
print(df)

j = int(((len(from_stmt_list) - 2)/4)) + 1
print(j)
counter = 0

for i in range(j):
    src_join_table = ''
    src_join_alias = ''
    src_join_field = ''
    trgt_join_table = ''
    trgt_join_alias = ''
    trgt_join_field = ''

    if i == 0:
        src_join_table = from_stmt_list[counter]
        counter += 1
        src_join_alias = from_stmt_list[counter]
        counter += 1

        # Calling the function for ControlTable validation check
        validation_check = control_table_validation_check(
            src_join_table, src_join_alias)

        df = df.append({'SrcJoinTable': src_join_table, 'SrcJoinAlias': src_join_alias, 'SrcJoinField': '',
                        'TrgtJoinTable': '', 'TrgtJoinAlias': '', 'TrgtJoinField': '', 'TableExistsInControl': validation_check[0],
                        'AliasAndTableMatch': validation_check[1]},
                       ignore_index=True)
    else:
        src_join_table = from_stmt_list[counter]
        counter += 1
        src_join_alias = from_stmt_list[counter]
        counter += 1
        trgt_join_field = from_stmt_list[counter]
        trgt_join_alias = trgt_join_field[:trgt_join_field.find('.')]
        trgt_join_field = trgt_join_field[trgt_join_field.find('.') + 1:]
        counter += 1
        src_join_field = from_stmt_list[counter]
        src_join_field = src_join_field[src_join_field.find('.') + 1:]
        counter += 1

        # Calling the function for ControlTable validation check
        validation_check = control_table_validation_check(
            src_join_table, src_join_alias)

        cursor.execute(
            "EXEC [TestResults].[LkpTblNameByAlias] @JoinAlias = '" + trgt_join_alias + " ' ")

        trgt_join_table = cursor.fetchone()[0]

        df = df.append({'SrcJoinTable': src_join_table, 'SrcJoinAlias': src_join_alias, 'SrcJoinField': src_join_field,
                       'TrgtJoinTable': trgt_join_table, 'TrgtJoinAlias': trgt_join_alias, 'TrgtJoinField': trgt_join_field,
                        'TableExistsInControl': validation_check[0], 'AliasAndTableMatch': validation_check[1]}, ignore_index=True)

# Close the sql connection
sql_conn.close()

# Enable output window to see all columns in  a dataframe
#pd.set_option('display.max_columns', None)
print(df)
