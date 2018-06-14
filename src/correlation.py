import os
import sys
import sqlite3
import pandas as pd
import numpy as np
import urllib, json

# Input files/URLs
hegedb = 'data/results_2018-04-01 00:00:00.sql'
arurl = 'http://as-rank-test.caida.org/api/v1/asns?populate=1&page=%s&sort=customer_cone_addresses'

# Store intermediate results here
hegecache = "data/20180401_AS_hegemony.csv"
arcache = "data/20180401_ASRank.csv"

###############################

def getHegemonyData(fin, fout):
    """ Transform AS hegemony raw results to csv. 
    """

    conn = sqlite3.connect(fin)
    #TODO remove hardcoded timestamp
    dfhege = pd.read_sql_query('SELECT asn, hege FROM hegemony WHERE scope=0 and ts=1522540800 and expid=1 ORDER BY hege DESC', conn)
    dfhege.to_csv(fout, index=False)

    return dfhege


def getASRankData(url, fout):
    data = None
    pagenb = 1
    rawdata = []

    while data is None or len(data) : 
        resp = urllib.urlopen(url % pagenb)
        page = json.loads(resp.read())
        data = page["data"]
        rawdata.extend(data)
        pagenb += 1

    df = pd.DataFrame([(v["id"], v["cone"]["asns"], v["cone"]["prefixes"], v["cone"]["addresses"]) for v in rawdata if set( ("asns", "prefixes", "addresses") ) <= set(v["cone"].keys())], columns=["asn", "cone_asns", "cone_prefixes", "cone_addresses"])

    df.to_csv(fout, index=False)

    return df

###############################

dfhege = None
dfar = None

# Read AS hegemony results
if not os.path.exists(hegecache):
    print("Reading AS hegemony results...")
    dfhege = getHegemonyData(hegedb, hegecache) 
else:
    print("Using cached results for AS hegemony")
    dfhege = pd.read_csv(hegecache)

# Fix problems with AS sets
dfhege["asn"] = pd.to_numeric(dfhege["asn"], errors="coerce")
dfhege = dfhege[dfhege["asn"].notnull()]
dfhege.index = dfhege["asn"].astype(int)


# Read AS Rank results
if not os.path.exists(arcache):
    print("Fetching AS Rank results...")
    dfar = getASRankData(arurl, arcache)
else:
    print("Using cached results for AS Rank")
    dfar = pd.read_csv(arcache)

dfar.index = dfar["asn"]

# Rank per hegemony / nb. IPs in cusotmer cone
dfhege["rank"] = dfhege.rank(ascending=False)["hege"]
dfar["rank"] = dfar.rank(ascending=False)["cone_addresses"]

# Group per log of rank
dfhege["logrank"] = np.log(dfhege["rank"]).astype(int)
dfar["logrank"] = np.log(dfar["rank"]).astype(int)

dfjoin = dfhege.join(dfar, how="inner", lsuffix="_hege", rsuffix="_ar")
corr = dfjoin.corr()
print corr
