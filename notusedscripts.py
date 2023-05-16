# Here inserted scipts that I wrote, but didn't use  


    #This api is for getting tickers types
# api = f"https://api.polygon.io/vX/reference/financials?ticker=AA&filing_date.gte={two_years_ago}&timeframe=annual&limit=100&apiKey={api_key}"
# request = requests.get(api).json()
# print (json.dumps(request, indent=3))
##### USE YOUR API KEY HERE. This is API for getting ticker types. Link
# url = f"https://api.polygon.io/v3/reference/tickers/types?asset_class=stocks&locale=us&apiKey={api_key}"
# response2 = requests.get(url).json()
# pd2 = pd.json_normalize(response2,"results")
# engine = create_engine(f"postgresql://{user}:{password}@{host}/{db_name}")
# pd2.to_sql("ticker_types", engine, if_exists="append", index = False)


    #This is API for getting all Tickers of common and preffered stock on US market. Link: 
# df = pd.DataFrame(columns=["cik", "name", "primary_exchange", "ticker", "type"])
# code = ["CS", "PFD"]
# for i  in code:
#     url = f"https://api.polygon.io/v3/reference/tickers?type={i}&active=false&limit=1000&apiKey={api_key}"
#     while True:
#         request = requests.get(url).json()
#         df2 = pd.json_normalize(request,"results")
#         if "composite_figi" in df2.columns:
#             df2 = df2.drop("composite_figi", axis = 1)
#         if "currency_name" in df2.columns:
#             df2 = df2.drop("currency_name", axis = 1)
#         if "market" in df2.columns:
#             df2 = df2.drop("market", axis = 1)
#         if "delisted_utc" in df2.columns:
#             df2 = df2.drop("delisted_utc", axis = 1)
#         if "last_updated_utc" in df2.columns:
#             df2 = df2.drop("last_updated_utc", axis = 1)
#         if "share_class_figi" in df2.columns:
#             df2 = df2.drop("share_class_figi", axis = 1)
#         if "locale" in df2.columns:
#             df2 = df2.drop("locale", axis = 1)
#         if "active" in df2.columns:
#             df2 = df2.drop("active", axis = 1)
#         df = pd.concat([df, df2], ignore_index=True)
#         time.sleep(12)
#         if "next_url" not in request:
#             break
#         url = request["next_url"] + "&apiKey=" + api_key
# df=df[~df["ticker"].duplicated()] #remove all duplicates from dataframe

# df.to_sql("tickers", engine, if_exists="append", index=False)