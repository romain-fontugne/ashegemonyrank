import sqlite3
import pandas as pd

# Read AS hegemony results

# dfhege = pd.read_sql_query('SELECT asn, hege FROM hegemony WHERE scope=0 and ts=1522540800 and expid=1 ORDER BY hege DESC')

dfhege = pd.read_csv("data/20180401_AS_hegemony_rank.csv", header=None, names=["rank","asn"])
print dfhege

# Read AS Rank results






def sqlite2csv():
    conn = sqlite3.connect('data/results_2018-04-01 00:00:00.sql')
    c = conn.cursor()
    for i, row in enumerate(c.execute('SELECT asn, hege FROM hegemony WHERE scope=0 and ts=1522540800 and expid=1 ORDER BY hege DESC')):
        print("%s,%s" % (i+1, row[0]))
