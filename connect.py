#! /usr/bin/python3.4
import os
import urllib.parse
import sys
import psycopg2
def heroku():
	#Connect to database
	urllib.parse.uses_netloc.append("postgres")
	url = urllib.parse.urlparse(os.environ["DATABASE_URL"])
	conn = psycopg2.connect(
	database=url.path[1:],
	user=url.username,
	password=url.password,
	host=url.hostname,
	port=url.port
	)
	return (conn)