#! /usr/bin/python3.4

import os
import sys
import time
import urllib.parse
import requests
import psycopg2
import connect
import re
from datetime import datetime, timedelta

def yahoo_api():

	while True:
		try:
			#init vars
			data = []
			stock_string = ""
			
			#Connect to database
			conn = connect.heroku()
			cursor = conn.cursor()

			#Pull symbols from portfolio
			cursor.execute("SELECT symbol FROM stock_info ORDER BY symbol asc;")
			rows = cursor.fetchall()
			for row in rows:
				stock_string += str(row)
			stock_string = re.sub(r'[()]?(,\)$)?',"",stock_string) #strip paraentheses and trailing comma
			
			#Pull stock info from Yahoo
			yql = 'select symbol, LastTradeDate, LastTradeWithTime, Change, ChangeinPercent from yahoo.finance.quotes \
			where symbol in ({})'.format(stock_string)
			yql = urllib.parse.quote(yql)
			url = 'https://query.yahooapis.com/v1/public/yql?q=' + yql + '&format=json&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys'
			response = requests.get(url)
			data = response.json()
			count = data['query']['count'] - 1

			#Generate query and update database values
			for i in data['query']['results']['quote']:
				symbol = i['symbol'].lower()
				price = re.search(r'<b>(.*)</b>', i['LastTradeWithTime']).group(1)
				change = i['Change'].strip("%+")
				change_per = i['ChangeinPercent'].strip("%+")
				SQL = "update stock_info set current_price = " + price + ", \
					last_updated = CURRENT_TIMESTAMP, change = " + change + ", \
					change_perc = " + change_per + " where symbol = '" + symbol + "';"

				#Check if Yahoo API updated, else ignore
				if re.search(r':',i['LastTradeWithTime']) != None:
					update = int(re.search(r'(.*):', i['LastTradeWithTime']).group(1)) + 4 #convert EST -> GMT
					now = (datetime.now() - timedelta(minutes=15)).hour
					if now > 12: now -= 12 #convert 24 hr -> 12 hr
					if update > 12: update -= 12
					if update == now:
						status = 1
					else: #fallback to date check
						update = datetime.strptime(i['LastTradeDate'], '%m/%d/%Y').strftime('%Y-%m-%d')
						now = str(datetime.now().date())
						if update == now:
							status = 1
						else:
							status = 0
				#if mutual fund perform only date check
				elif re.search(r'x$', symbol) != None or datetime.now().hour > 15:
					update = datetime.strptime(i['LastTradeDate'], '%m/%d/%Y').strftime('%Y-%m-%d')
					now = str(datetime.now().date())
					if update == now:
						status = 1
					else:
						status = 0
				else:
					update = "null"
					now = "null"
					status = 0
				if status == 1:
					print ("\n{} updated".format(symbol.upper()))
					print ("{} == {}".format(update, now))	
					cursor.execute(SQL)
					print ("psql : ", cursor.statusmessage)
					conn.commit()
				elif status == 0:
					print ("\n{} not updated".format(symbol.upper()))
					print ("{} != {}".format(update, now))
				else: print ("ERROR - STATUS NULL")

				print ("{} : {} : {}".format(price, change, change_per))

					
			conn.close()                    
			print ("\nClosing Connection")
			print ("Time =", datetime.now().time(), "\n\n")
			time.sleep(20)
	
		except:
			print ("Unexpected error:", sys.exc_info())
			print (i, "\n")
			print ("Data:", data)
			time.sleep(20)
			continue
	 
if __name__ == "__main__":
	yahoo_api()
