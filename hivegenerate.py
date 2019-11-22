import pandas as pd
import re


def generate_hive_meta(table):
    table.name = input("input hive table target: ") 
    dict = {'int64':'int','object':'string','float64':'double',}
    col_type_temp = [ ]
    for i in range(len(table.keys())):
        a = table.keys()[i] +" "+ dict[str(table.dtypes[i])]
        col_type_temp.append(a)
    col_type = re.sub("\'|\[|\]", "", str(col_type_temp))
    command = "CREATE EXTERNAL TABLE IF NOT EXISTS home_credit_muhammadrizki."+table.name+"(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' location 'hdfs:///user/insiderhack/HomeCredit/%s';" % (col_type, table.name) #seetlike ur params and want to formatted
    return command