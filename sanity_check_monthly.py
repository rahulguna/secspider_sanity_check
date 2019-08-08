#!/usr/bin/python
import pymysql as mariadb
import pymysql.cursors
import configparser
import datetime
from datetime import timezone
import sys
import random 
from hashlib import md5
from base64 import b64decode
from base64 import b64encode
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from Crypto.Util.Padding import pad, unpad

#configuration settings
config = configparser.ConfigParser()
config.read('config.ini')

print("Secspider Sanity Check\n")

#decrypt password
passkey = 'rfherf34843h34hj83f'
enrypted_password = config['DATABASE']['PASSWORD']

key = md5(passkey.encode('utf8')).digest()       
raw = b64decode(enrypted_password)
cipher = AES.new(key, AES.MODE_CBC, raw[:AES.block_size])
password=unpad(cipher.decrypt(raw[AES.block_size:]), AES.block_size).decode('utf-8')

#database connection
try:
	user = config['DATABASE']['USER'] # 'username for database'
	database_name = config['DATABASE']['DATABASE_NAME'] # 'database name'
	print("--> Connecting to database - "+database_name+"\n")
	mariadb_connection = mariadb.connect(user=user, password=password, database=database_name)
	cursor = mariadb_connection.cursor()
except:
	print("Access denied or config file not found\n")
	sys.exit(1)

def main():

	#Calculate of expected behaviour and sanity check for SS_RRSET table
	range_arr=calculate_expected_behaviour('SS_RRSET')
	check_sanity('SS_RRSET', range_arr)
	
	mariadb_connection.close()

def calculate_expected_behaviour(str1):

	today = datetime.date.today()
	one_month = datetime.timedelta(days=31)
	two_month = datetime.timedelta(days=62)
	three_month = datetime.timedelta(days=92)
	one_month_back = today - one_month
	two_month_back = today - two_month
	three_month_back = today - three_month
	
	last_month_total_count = 0
	two_month_total_count = 0
	three_month_total_count = 0
	last_month_zone_count = 0
	two_month_zone_count = 0
	three_month_zone_count = 0

	#one_month_back = datetime.date(2018, 10, 21)
	#two_month_back = datetime.date(2018, 7, 21)
	#three_month_back = datetime.date(2018, 4, 21)

	select_query_total_count = "SELECT COUNT(ID) AS COUNT FROM "+str1+" WHERE YEAR(FROM_UNIXTIME(FIRST_SEEN)) = %s AND MONTH(FROM_UNIXTIME(FIRST_SEEN)) = %s"
	cursor.execute(select_query_total_count, (one_month_back.year,one_month_back.month))
	for COUNT in cursor:
		last_month_total_count = COUNT[0]
	cursor.execute(select_query_total_count, (two_month_back.year,two_month_back.month))
	for COUNT in cursor:
		two_month_total_count = COUNT[0]
	cursor.execute(select_query_total_count, (three_month_back.year,three_month_back.month))
	for COUNT in cursor:
		three_month_total_count = COUNT[0]
	
	temp = last_month_total_count + two_month_total_count + three_month_total_count
	if(temp != 0):
		avg_total_count = int(temp/3)
	else:
		print("--> No sufficient data available to calculate for the expected behaviour for "+str1+" table\n")
		return []
	
	select_query_zone_count = "SELECT NO_OF_ROWS from SS_TABLE_SANITY_CHECK where TABLE_NAME='SS_ZONE' and YEAR(FROM_UNIXTIME(TIMESTAMP)) = %s AND MONTH(FROM_UNIXTIME(TIMESTAMP)) = %s order by ID desc limit 1;"
	cursor.execute(select_query_zone_count, (today.year,today.month))
	for NO_OF_ROWS in cursor:
	    curr_month_zone_count = NO_OF_ROWS[0]
	cursor.execute(select_query_zone_count, (one_month_back.year,one_month_back.month))
	for NO_OF_ROWS in cursor:
	    last_month_zone_count = NO_OF_ROWS[0]
	cursor.execute(select_query_zone_count, (two_month_back.year,two_month_back.month))
	for NO_OF_ROWS in cursor:
	    two_month_zone_count = NO_OF_ROWS[0]
	cursor.execute(select_query_zone_count, (three_month_back.year,three_month_back.month))
	for NO_OF_ROWS in cursor:
		three_month_zone_count = NO_OF_ROWS[0]

	if(last_month_zone_count == 0 or two_month_zone_count == 0 or three_month_zone_count == 0):
		avg_count_of_zone = curr_month_zone_count
	else:
		temp2 = last_month_zone_count + two_month_zone_count + three_month_zone_count
		avg_count_of_zone = int(temp2/3)
	
	temp1 = int(avg_total_count/avg_count_of_zone)
	expected_total_count = temp1*curr_month_zone_count
	print(avg_total_count, avg_count_of_zone, expected_total_count)

	if((expected_total_count - 100) < 0):
		ROWS_COUNT_FROM = 0
	else:
		ROWS_COUNT_FROM = expected_total_count - 100

	ROWS_COUNT_TO = expected_total_count + 100

	expected_range_arr = [ROWS_COUNT_FROM, ROWS_COUNT_TO]
	print("--> Calculated expected range for "+str1+" : ",expected_range_arr)
	return expected_range_arr

def check_sanity(str1, range_arr):
	print("--> Sanity Check for "+str1+" table")

	year = datetime.date.today().year
	month = datetime.date.today().month
	#year = 2018
	#month = 10

	TOTAL_COUNT = 0

	select_query_total_count = "SELECT COUNT(ID) AS COUNT FROM "+str1+" WHERE YEAR(FROM_UNIXTIME(FIRST_SEEN)) = %s AND MONTH(FROM_UNIXTIME(FIRST_SEEN)) = %s"
	cursor.execute(select_query_total_count, (year,month))
	for COUNT in cursor:
		TOTAL_COUNT = COUNT[0]

	print("--> Total number of rows inserted in "+str1+" table: ",TOTAL_COUNT)
	
	EXPECTED_COUNT_FROM = range_arr[0]
	EXPECTED_COUNT_TO = range_arr[1]

	if TOTAL_COUNT<EXPECTED_COUNT_FROM or TOTAL_COUNT>EXPECTED_COUNT_TO:
		print("--> The sanity check for "+str1+": FAILED\n")
		return


if __name__== "__main__":
	main()
