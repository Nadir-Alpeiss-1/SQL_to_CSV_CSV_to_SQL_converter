import sqlite3
import csv

import sqlite3 as sq
import pandas as pd
from io import StringIO

def sql_to_csv(database, table_name):
    
    try:
        conn = sqlite3.connect(database)
        print("database is opened")
    except Exception as e:
        print("error during connection", str(e))
    results = conn.execute("SELECT * FROM " + table_name)
    
    #header of the sql table
    names = [description[0] for description in results.description]

    #selecting header - column names
    CSV = ""
    header = ""
    for j in names:
        header += j
        header += ","

    header_1 = header[0:-1] + "\n"

    CSV = header_1

    #creating a CSV string
    
    for row in results:
        #converting all the types of data (int etc) of the list into string type by map
        my_string = ','.join(map(str, row))
        CSV += my_string
        CSV += '\n'
    
    # deleting the last '\n' sign 
    CSV_f = CSV[0:-1]
    return CSV_f
    
    # implementing & closing connection to SQLite database
    conn.execute()
    conn.close()

#to SEE the RESULTS - delete # sign in front of "print(sql_to_csv()"
#print(sql_to_csv('all_fault_line.db','fault_lines'))

csv_content = open("list_volcano.csv")

def csv_to_sql(csv_content, database, table_name):
    
    # creating a connection object
    connection = sq.connect(database)
    # creating a cursor object
    curs = connection.cursor()
    
    #reading csv file
    data = csv_content.read()
    #print(data)
    
    #creating header for the sql table
    data_h = ""
    for i in data:
        if i != '\n':
            data_h += i
        else:
            break
            
    res = data_h.split(',')
    res_f = []
    
    for i in res:
        i = i.replace(" ", "_")
        i = i.replace(")", "")
        i = i.replace("(", "")
        res_f.append(i)
    
    res_f_s = ""
    for i in res_f:
        res_f_s += i
        res_f_s += ", "
    
    res_f_s_1 = res_f_s[0:-2]
    
    # running and creation of table sql query
    
    curs.execute("CREATE TABLE if not Exists " + table_name +

                 "(" + res_f_s_1 + ")") 
    
  
    #loading CSV data into Pandas DataFrame
    TESTDATA = StringIO(data)
    
    df = pd.read_csv(TESTDATA, sep=",")
     
    # writing the data to a sqlite db table
    df.to_sql(table_name, connection, if_exists='replace', index=False)

    # running and selecting sql query
    curs.execute('select * from ' + table_name)
    
    #Displaying the results - DELETE # sign in front of "for row in records" & "print(row)" - TO SEE THE RESULTS 
    #for row in records:
        # show row
        #print(row)
    
    # closing CSV file & implementing & closing connection to SQLite database
    csv_content.close()
    connection.commit()
    connection.close()

# to SEE the RESULTS delete # sign in front of "csv_to_sql()" 
#csv_to_sql(csv_content, 'list_volcano.db','volcanos')  