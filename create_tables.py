import psycopg2, requests, pandas as pd
from config import host, user, password, db_name, api_key
from sqlalchemy import create_engine
from datetime import date
from dateutil.relativedelta import relativedelta
import time
import json
from bs4 import BeautifulSoup
import lxml

# Get today date and two years ago date, needed for further tasks
today = date.today()
two_years_ago = today - relativedelta(years=2) + relativedelta(days=1)

# define function set_connection to set connection to database 
def set_connection ():
    connection = psycopg2.connect(database = db_name,
    host = host,
    user = user,
    password = password     
    )
    return connection

# "sqlalchemy" engine to connect to database 
engine = create_engine(f"postgresql://{user}:{password}@{host}/{db_name}")
connection = set_connection()   

# create tables in PostgreSQL database that we need 
try:
    connection.autocommit = True

    # create cursor is used for perfoming database operations
    with connection.cursor() as cursor:
        cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS market_cap(
        "ticker" varchar(20),
        "market_cap" float
        );
        CREATE TABLE IF NOT EXISTS bars_daily(
        "ticker" varchar(10) NOT NULL,
        "close" float,
        "high" float,
        "low" float,
        "open" float,
        "time" timestamp,
        "volume" bigint);
        CREATE TABLE IF NOT EXISTS metrics_fin(
        "ticker" varchar(15),
        "metrics" varchar(100),
        "name" varchar(30)
        );
        CREATE TABLE IF NOT EXISTS ticker_types (
        "code" varchar(20) NOT NULL,
        "description" varchar NOT NULL,
        "asset_class" varchar(15) NOT NULL,
        "locale" varchar(15) NOT NULL);
        CREATE TABLE IF NOT EXISTS tickers("ticker" varchar(15));
        CREATE TABLE IF NOT EXISTS stock_financials(
        "ticker" varchar(15),
        "fiscal_year" varchar(4),
        "value" float,
        "unit" varchar(30),
        "label" varchar,
        "order" int,
        "form" varchar(200)
        );
        CREATE TABLE IF NOT EXISTS empty_bars_daily(
        "empty_bars_daily" varchar(15)
        );
        CREATE TABLE IF NOT EXISTS empty_stock_financials(
        "empty_stock_financials" varchar(15)
        )
        """
        )
except Exception as ex_e:
    print ("[Info] error occured while working to database:", ex_e)

# Web scrapping of tickeres included in Nasdaq 100
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.5672.64 Safari/537.36'}

list_of_endpoints = ["", "&r=21", "&r=41", "&r=61", "&r=81"]
list_of_tickers = []
sleep_period = 0
for i in list_of_endpoints:
    file_to_parse = requests.get(f"https://finviz.com/screener.ashx?v=111&f=exch_nasd,geo_usa,idx_ndx{i}", headers=headers).text
    soup = BeautifulSoup(file_to_parse, "lxml")
    lines_tr_full = soup.find_all("tr", valign = "top")
    for lines_tr in lines_tr_full:
        lines_td = lines_tr.find("td", align = "left").a.text
        list_of_tickers.append(lines_td)
    sleep_period+=2
    time.sleep(sleep_period) 
df = pd.DataFrame(list_of_tickers, columns=["ticker"])
df.to_sql("tickers", engine, if_exists="append", index=False)

# Get from database the list of all tickers 
try:
    with connection.cursor() as cursor:
        cursor.execute("SELECT ticker FROM tickers")
        alltickers = cursor.fetchall()
except Exception as exc:
    print (f"INFO: Exception occured while woring to database {exc}")

alltickers2 = [i[0] for i in alltickers]

# Get metrics for all the tickers
list_of_metrics = []
tickers_for_list_of_metrics = []
names_for_list_of_metrics = []
for ticker in alltickers2:
    file_to_parse = requests.get(f"https://finviz.com/quote.ashx?t={ticker}&p=d", headers=headers).text
    soup = BeautifulSoup(file_to_parse, "lxml")
    lines_tr_full = soup.find_all("td", class_ = "snapshot-td2")
    for i in lines_tr_full:
        tickers_for_list_of_metrics.append(ticker)
        list_of_metrics.append(i.find("b").text)
    lines_tr_full = soup.find_all("td", class_ = "snapshot-td2-cp")
    for i in lines_tr_full:
        names_for_list_of_metrics.append(i.text)
    time.sleep(3)

# Get capitalization info without "B" at the end of each string
cap_list = []
tickers_for_cap_list = []
for i in range(6, len(list_of_metrics), 72):
    cap_list.append(list_of_metrics[i][:-1])
for i in range(6, len(tickers_for_list_of_metrics),72):
    tickers_for_cap_list.append(tickers_for_list_of_metrics[i])

# Add metrics to database
df = pd.DataFrame({'ticker':tickers_for_cap_list, 'market_cap':cap_list})
df.to_sql("market_cap", engine, if_exists="append", index=False)

df = pd.DataFrame({'ticker':tickers_for_list_of_metrics, 'metrics':list_of_metrics, 'name':names_for_list_of_metrics})
df.to_sql("metrics_fin", engine, if_exists="append", index=False)

# This is API request for getting Aggregates(bars). Link: https://polygon.io/docs/stocks/get_v2_aggs_ticker__stocksticker__range__multiplier___timespan___from___to

df2 = pd.DataFrame(columns=["ticker","close","high","low","open","time","volume"])
empty_tickers = []

for ticker in alltickers2:
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{two_years_ago}/{today}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"
    response = requests.get(url).json()
    time.sleep(12)
    if "results" not in response.keys(): 
        empty_tickers.append(ticker)
        continue
    if response["results"] == []:
        empty_tickers.append(ticker)
        continue
    df = pd.json_normalize(response,"results", "ticker")
    df = df.drop("vw", axis=1)
    df = df[["ticker","c","h","l","o","t","v"]]
    df = df.rename(columns={"c":"close","h":"high","l":"low","o":"open","t":"time","v":"volume"})
    df2 = pd.concat([df2, df], ignore_index=True)

df2["time"]=pd.to_datetime(df2["time"],unit='ms')
df2.to_sql("bars_daily", engine, if_exists="append", index=False) #add Aggregates(bars) to databbase
df3 = pd.DataFrame(empty_tickers, columns=["empty_bars_daily"])
df3.to_sql("empty_bars_daily", engine, if_exists="append", index=False) #add tickers with no data to database


# This is API request for getting Stock financial. Link: https://polygon.io/docs/stocks/get_vx_reference_financials

mylist = [[], [], [], [], [], [], []]
empty_tickers = []

for l in alltickers2:
    api = f"https://api.polygon.io/vX/reference/financials?ticker={l}&filing_date.gte={two_years_ago}&timeframe=annual&limit=100&apiKey={api_key}"
    request = requests.get(api).json()
    time.sleep(12)
    if "results" not in request.keys():
        empty_tickers.append(l)
        continue
    if request["results"] == []:
        empty_tickers.append(l)
        continue
    for k  in request["results"]:
        p=0
        for s in k["financials"]:
            for i, n in enumerate(k["financials"][s]):
                mylist[0].append(l)
                mylist[1].append(k['fiscal_year'])
                mylist[2].append(k['financials'][s][n]["value"])
                mylist[3].append(k['financials'][s][n]["unit"])
                mylist[4].append(k['financials'][s][n]["label"])
                mylist[5].append(k['financials'][s][n]["order"])
                mylist[6].append(list(k["financials"].keys())[p])
                if i == len(k["financials"][s])-1:
                    p+=1
                    continue
                                 
df = pd.DataFrame({"ticker":mylist[0], 'fiscal_year':mylist[1], "value":mylist[2], "unit":mylist[3], "label": mylist[4], "order": mylist[5], "form": mylist[6]})
df2 = pd.DataFrame(empty_tickers, columns=["empty_stock_financials"])
df.to_sql("stock_financials", engine, if_exists="append", index=False) #paste in database
df2.to_sql("empty_stock_financials", engine, if_exists="append", index=False)




















if connection:
    connection.close()
    print("[INFO] the connection was closed")

