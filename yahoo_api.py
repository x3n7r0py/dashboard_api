#! /usr/bin/python3.4

import os
import sys
import time
import urllib.parse
import requests
import psycopg2
import connect
import re
import json
from datetime import datetime, timedelta

def yahoo_api():

	while True:
		try:
			#Connect to database
			now = datetime.now()
			mktopen = now.replace(hour=14, minute=45, second=0, microsecond=0)
			mktclose = now.replace(hour=20, minute=30, second=0, microsecond=0)
			print("Time is "+str(now))
			print ("Is market open? " + str(now > mktopen and now < mktclose))
			if now < mktopen or now > mktclose:
				print("Waiting 30 min.....")
				time.sleep(900)
				continue
			conn = connect.heroku()
			cursor = conn.cursor()
			
			#Pull global stock and index info from Yahoo
			cursor.execute("SELECT symbol FROM stock_info where type <> 'equity' ORDER BY symbol asc;")
			symbols = ""
			for row in cursor.fetchall():
				symbols += "'"+row[0]+"',"         
			symbols = re.sub(r',$',"",symbols)
			yql = 'select symbol, LastTradeDate, LastTradeWithTime, Change, ChangeinPercent from yahoo.finance.quotes \
			where symbol in ({})'.format(symbols)
			yql = urllib.parse.quote(yql)
			url = 'https://query.yahooapis.com/v1/public/yql?q=' + yql + '&format=json&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys'
			response = requests.get(url)
			data = response.json()["query"]["results"]["quote"]

			#Generate query and update database values
			for stock in data:
				symbol = stock['symbol'].lower()
				lastPrice = re.search(r'<b>(.*)</b>', stock['LastTradeWithTime']).group(1)
				netChange = stock['Change'].strip("%+")
				percentChange = stock['ChangeinPercent'].strip("%+")
				SQL = "update stock_info set current_price = " + lastPrice + ", \
					last_updated = CURRENT_TIMESTAMP, change = " + netChange + ", \
					change_perc = " + percentChange + " where symbol = '" + symbol + "';"
				print ("{} : {} : {} : {}".format(symbol, lastPrice, netChange, percentChange))		
				cursor.execute(SQL)
				conn.commit()
			
			#Pull stock info from MarketData
			cursor.execute("SELECT symbol FROM stock_info where type = 'equity' ORDER BY symbol asc;")
			symbols = "";
			for row in cursor.fetchall():
				symbols += row[0]+","         
			url = "http://marketdata.websol.barchart.com/getQuote.json?key=c220e3123aa026d0a8a8e958f9f1672a&symbols=" + symbols
			response = requests.get(url)
			data = response.json()['results']
			for stock in data:
				symbol = stock["symbol"].lower()
				lastPrice = str((stock["lastPrice"]))
				netChange = str((stock["netChange"]))
				percentChange = str((stock["percentChange"]))
				SQL = "update stock_info set current_price = " + lastPrice + ", \
					last_updated = CURRENT_TIMESTAMP, change = " + netChange + ", \
					change_perc = " + percentChange + " where symbol = '" + symbol + "';"
				print ("{} : {} : {} : {}".format(symbol, lastPrice, netChange, percentChange))
				cursor.execute(SQL)
				conn.commit()

			print ("\nClosing Connection")
			print ("Time =", datetime.now().time(), "\n\n")
			conn.close()
			time.sleep(300)
	
		except:
			print ("Unexpected error:", sys.exc_info())
			time.sleep(300)
			continue
	 
if __name__ == "__main__":
	yahoo_api()
