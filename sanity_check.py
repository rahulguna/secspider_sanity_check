#!/usr/bin/python
import pymysql as mariadb
import pymysql.cursors
import configparser
import datetime
from datetime import timezone
import sys
import random 
import logging
from hashlib import md5
from base64 import b64decode
from base64 import b64encode
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from Crypto.Util.Padding import pad, unpad

#configuration settings
config = configparser.ConfigParser()
config.read('config.ini')

log_level = config['LOG']['LEVEL']
logging.basicConfig(
	filename="sanity_check.log",
	level=log_level,
	filemode='w',
	format="%(asctime)s:%(levelname)s:[%(filename)s:%(lineno)d]:%(message)s"
    )

logging.debug("Secspider Sanity Check\n")

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
	logging.debug("--> Connecting to database - "+database_name+"\n")
	mariadb_connection = mariadb.connect(user=user, password=password, database=database_name)
	cursor = mariadb_connection.cursor()
except:
	print("Access denied or config file not found\n")
	logging.error("Access denied or config file not found\n")
	sys.exit(1)

#global variables
curr_run=0;

table_name_arr = ['SS_RRSET','SS_EXP_RRSET','SS_RRSIG','SS_EXP_RRSIG',
		'SS_RR','SS_EXP_RR','SS_DNSKEY','SS_EXP_DNSKEY',
		'SS_DS','SS_EXP_DS','SS_TLSA','SS_EXP_TLSA','SS_RRSET_EXP_REL',
		'SS_RUN_STATS', 'SS_ZONE_STATS', 'SS_KEY_STATS', 'SS_NAMESERVER_STATS', 'SS_TLSA_STATS','SS_ZONE']

table_count_arr = []

def main():

	insert_count() #Insert table count to SS_TABLE_SANITY_CHECK table
	retrieve_prev_run_count() #Retrieve table count for previous run from SS_TABLE_SANITY_CHECK table
	
	#Calculate rows inserted for SS_RRSET, SS_EXP_RRSET, SS_RRSIG, SS_EXP_RRSIG, SS_RR, SS_EXP_RR, SS_DNSKEY, SS_EXP_DNSKEY, SS_DS, SS_EXP_DS, SS_TLSA, SS_EXP_TLSA
	index1=0
	while(index1 < 11): 
		calculate_rows_inserted(index1, table_name_arr[index1], index1+1, table_name_arr[index1+1])
		index1 += 2

	#Calculate rows inserted for SS_RRSET_EXP_REL and STATS table
	index2 = 12
	while(index2 <= 17): 
		calculate_rows_inserted_stats_table(index2, table_name_arr[index2])
		index2 += 1

	#Sanity check for SS_RRSIG, SS_EXP_RRSIG, SS_RR, SS_EXP_RR, SS_DNSKEY, SS_EXP_DNSKEY, SS_DS, SS_EXP_DS, SS_TLSA, SS_EXP_TLSA
	index3=0
	while(index3 < 11): 
		check_sanity_set_tables(index3, table_name_arr[index3], index3+1, table_name_arr[index3+1])
		index3 += 2
	
	#Calculate of expected behaviour and sanity check for STATS table
	index4 = 14
	while(index4 <= 17): 
		check_stats_tables(index4, table_name_arr[index4])
		index4 += 1

	#Sanity check for SS_RRSET_EXP_REL table
	check_RRSET_EXP_REL(12, table_name_arr[12])
	
	#Check referential integrity
	check_referential_integrity(0, table_name_arr[0], 1, table_name_arr[1]);

	logging.debug("[Table_Name, #Rows in current run, #Rows in previous run, Rows inserted]")
	for item in table_count_arr:
		logging.debug(item)
	logging.debug("-------------------------------------------------------------------------------\n")

	mariadb_connection.close()

def insert_count():		#Insert table count to SS_TABLE_SANITY_CHECK table
	global curr_run
	
	last_run=0
	cursor.execute("SELECT RUN_ID from SS_TABLE_SANITY_CHECK order by ID desc LIMIT 1")
	for RUN_ID in cursor:
		last_run=RUN_ID[0]

	curr_run = last_run+1;
	
	for item in table_name_arr:
		insert_into_sanity_table(item)

	logging.debug("--> The data for the lastest run is inserted into sanity check table\n")

def insert_into_sanity_table(table_name):	#Insert table count to SS_TABLE_SANITY_CHECK table

	try:
		sql_parameterized_insert_query = """INSERT INTO SS_TABLE_SANITY_CHECK (RUN_ID,TABLE_NAME,TIMESTAMP,NO_OF_ROWS) VALUES (%s,%s,%s,%s)"""
		select_query="SELECT COUNT(ID) AS COUNT from "+table_name
		cursor.execute(select_query)
		for COUNT in cursor:
			table_list_count = [table_name, COUNT[0], 0, 0] 
		table_count_arr.append(table_list_count)
		insert_values=(curr_run, table_name, datetime.datetime.now().strftime('%s'), table_list_count[1])
		cursor.execute(sql_parameterized_insert_query, insert_values)
		mariadb_connection.commit()

	except mariadb.Error as error:
		print("Error: {}".format(error))
		mariadb_connection.rollback()

def retrieve_prev_run_count():		#Retrieve table count for previous run from SS_TABLE_SANITY_CHECK table
	
	prev_run=0
	cursor.execute("SELECT DISTINCT(RUN_ID) from SS_TABLE_SANITY_CHECK order by ID desc LIMIT 1,1")
	runs = cursor.fetchall()
	if(len(runs)==0):
		print("--> There are no previous data in the sanity check table. Please run the sanity check again\n")
		logging.error("--> There are no previous data in the sanity check table. Please run the sanity check again\n")
		sys.exit(0)
	for ID in runs:
		prev_run=ID[0]

	cursor.execute("SELECT TABLE_NAME,NO_OF_ROWS from SS_TABLE_SANITY_CHECK where RUN_ID=%s",(prev_run))
	records = cursor.fetchall()
	for row in records:
		i=0
		while (i < len(table_count_arr)):
			if table_count_arr[i][0]==row[0]:
				table_count_arr[i][2]=row[1]
			i += 1

def calculate_rows_inserted(a, str1, b, str2):		#calculates rows inserted for RRSET tables
	if table_count_arr[b][0]==str2:
		table_count_arr[b][3]=table_count_arr[b][1]-table_count_arr[b][2]
	if table_count_arr[a][0]==str1:
		result=table_count_arr[a][2]-table_count_arr[b][3]
		table_count_arr[a][3]=table_count_arr[a][1]-result

def calculate_rows_inserted_stats_table(a, str1):		#calculates rows inserted for other tables
	if table_count_arr[a][0]==str1:
		table_count_arr[a][3]=table_count_arr[a][1]-table_count_arr[a][2]

def check_sanity_set_tables(a, str1, b, str2):
	status=1
	if(table_count_arr[b][3] < 0):
		status=0
		print("--> Rows have been dropped from "+str2+" table\n")
		logging.error("--> Rows have been dropped from "+str2+" table\n")
	if(table_count_arr[a][3] < 0):
		status=0
		print("--> Rows have been dropped from "+str1+" table\n")
		logging.error("--> Rows have been dropped from "+str1+" table\n")
	
	if(a==0 and b==1):
		status=0
		if(table_count_arr[a][3] > 0):
			index=2
			while(index<=10):
				if(table_count_arr[index][3] > 0):
					status=1
				index +=2
		elif(table_count_arr[a][3] == 0):
			index=2
			while(index<=10):
				if(table_count_arr[index][3] != 0):
					print("--> The sanity check for "+str1+": FAILED - Condition 1\n")
					logging.error("--> The sanity check for "+str1+": FAILED - Condition 1\n")
					return
				else:
					status=1
				index +=2
		
		#checks whether rows are getting updated in SS_RRSET table
		today = datetime.date.today()
		updated_rows_count = 0
		select_query_updated_count = "SELECT COUNT(ID) FROM "+str1+" WHERE YEAR(FROM_UNIXTIME(LAST_SEEN)) = %s AND MONTH(FROM_UNIXTIME(LAST_SEEN)) = %s AND DAY(FROM_UNIXTIME(LAST_SEEN)) = %s AND FIRST_SEEN!=LAST_SEEN;"
		cursor.execute(select_query_updated_count, (today.year,today.month,today.day))
		for ID in cursor:
			updated_rows_count=ID[0]
		if(updated_rows_count==0):
			print("--> No new rows were updated in "+str1+" table\n")
			logging.error("--> No new rows were updated in "+str1+" table\n")

		#checks whether FIRST SEEN and LAST SEEN are same in SS_EXP_RRSET table
		today = datetime.date.today()
		rows_not_same_count = 0
		rows_same_count = 0
		select_query_check_not_same_count = "SELECT COUNT(ID) FROM "+str2+" WHERE YEAR(FROM_UNIXTIME(LAST_SEEN)) = %s AND MONTH(FROM_UNIXTIME(LAST_SEEN)) = %s AND FIRST_SEEN!=LAST_SEEN;"
		select_query_check_same_count = "SELECT COUNT(ID) FROM "+str2+" WHERE YEAR(FROM_UNIXTIME(LAST_SEEN)) = %s AND MONTH(FROM_UNIXTIME(LAST_SEEN)) = %s;"
		
		cursor.execute(select_query_check_not_same_count, (today.year,today.month))
		for ID in cursor:
			rows_not_same_count=ID[0]

		cursor.execute(select_query_check_same_count, (today.year,today.month))
		for ID in cursor:
			rows_same_count=ID[0]
		if(rows_same_count!=rows_not_same_count):
			print("--> All rows in "+str2+" table have the same FIRST SEEN and LAST SEEN values\n")
			logging.error("--> All rows in "+str2+" table have the same FIRST SEEN and LAST SEEN values\n")
		
	else:
		if(table_count_arr[a][3] > 0):
			if(table_count_arr[0][3] > 0):
				pass
			else:
				status=0
				print("--> "+str1+" tables has values inserted in it, but "+table_count_arr[0][0]+" does not have values inserted\n")
				logging.error("--> "+str1+" tables has values inserted in it, but "+table_count_arr[0][0]+" does not have values inserted\n")


	if status==1:
		pass
		#print("--> The sanity check for "+str1+": PASSED\n")
	else:
		print("--> The sanity check for "+str1+": FAILED\n")
		logging.error("--> The sanity check for "+str1+": FAILED\n")

def	check_stats_tables(a, str1):
	if table_count_arr[a][0]==str1:
		if(table_count_arr[a][3] < 0):
			print("--> Rows have been dropped from "+str1+" table\n")
			logging.error("--> Rows have been dropped from "+str1+" table\n")

	if table_count_arr[0][3] > 0:
		if(table_count_arr[a][3] == 0):
			print("--> No new insertions in "+str1+" table\n")
			logging.error("--> No new insertions in "+str1+" table\n")

def check_RRSET_EXP_REL(a, str1):
	status=1
	if(table_count_arr[a][3] < 0):
		status=0
		print("--> Rows have been dropped from "+str1+" table\n")
		logging.error("--> Rows have been dropped from "+str1+" table\n")

	month=datetime.datetime.now().strftime('%m')
	cursor.execute("SELECT COUNT(ID) from SS_RRSET_EXP_REL where EXP_SET_ID is not null and MONTH(FROM_UNIXTIME(SEEN)) = %s",(month))
	count_exp_id = 0
	for ID in cursor:
		count_exp_id = ID[0]
	if(count_exp_id == 0):
		status=0
		print("--> EXP_SET_ID column in "+str1+" is not being set\n")
		logging.error("--> EXP_SET_ID column in "+str1+" is not being set\n")

	if(table_count_arr[a][3] > 0):
		if(table_count_arr[0][3] > 0):
			pass
		else:
			status=0
			print("--> "+str1+" tables has values inserted in it, but "+table_count_arr[0][0]+" does not have values inserted\n")
			logging.error("--> "+str1+" tables has values inserted in it, but "+table_count_arr[0][0]+" does not have values inserted\n")

	if status==1:
		pass
		#print("--> The sanity check for "+str1+": PASSED\n")
	else:
		print("--> The sanity check for "+str1+": FAILED\n")
		logging.error("--> The sanity check for "+str1+": FAILED\n")

def check_referential_integrity(a, str1, b, str2):
	today = datetime.date.today()
	yesterday = datetime.timedelta(days=1)
	max_rrset = 0
	max_exp_rrset = 0

	select_query_rrset_count = "SELECT max(ID) from "+str1
	cursor.execute(select_query_rrset_count)
	for item in cursor:
		max_rrset = item[0]

	select_query_exp_rrset_count = "SELECT max(ID) from "+str2
	cursor.execute(select_query_exp_rrset_count)
	for item in cursor:
		max_exp_rrset = item[0]

	if(table_count_arr[a][3]<0 or table_count_arr[b][3]<0):
		print("--> Referential integrity not checked. Rows have been dropped")
		logging.error("--> Referential integrity not checked. Rows have been dropped")
		return

	while True:
		rrset_id=0
		rrset_rr_type=0
		while True:
			random1 = random.randint(max_rrset-table_count_arr[a][3], max_rrset) 
			select_query_rrset = "SELECT ID,RR_TYPE from "+str1+" WHERE ID=%s"
			cursor.execute(select_query_rrset, (random1))
			result1 = cursor.fetchone()
			if(len(result1)!=0):
				rrset_id = result1[0]
				rrset_rr_type = result1[1]
				break

		join_table_rrset = ""
		
		if(rrset_rr_type == 48):
			join_table_rrset = table_name_arr[6]
		elif(rrset_rr_type == 43):
			join_table_rrset = table_name_arr[8]
		elif(rrset_rr_type == 52):
			join_table_rrset = table_name_arr[10]
		elif(rrset_rr_type == 2 or rrset_rr_type == 6):
			join_table_rrset = table_name_arr[4]

		join_query_rrset = "SELECT "+table_name_arr[0]+".ID, "+table_name_arr[0]+".RR_TYPE from "+table_name_arr[0]+" inner join "+join_table_rrset+" on "+table_name_arr[0]+".ID = "+join_table_rrset+".SET_ID inner join "+table_name_arr[2]+" on "+table_name_arr[2]+".SET_ID = "+join_table_rrset+".SET_ID where "+table_name_arr[0]+".ID=%s"
		join_query_rrset_without_rrsig = "SELECT "+table_name_arr[0]+".ID, "+table_name_arr[0]+".RR_TYPE from "+table_name_arr[0]+" inner join "+join_table_rrset+" on "+table_name_arr[0]+".ID = "+join_table_rrset+".SET_ID where "+table_name_arr[0]+".ID=%s"
		logging.debug("--> The SS_RRSET ID tested for referential integrity: %s",rrset_id)
		cursor.execute(join_query_rrset, (rrset_id))
		result2 = cursor.fetchall()
		logging.debug(result2)
		if(len(result2)==0):
			cursor.execute(join_query_rrset_without_rrsig, (rrset_id))
			result3 = cursor.fetchall()
			logging.debug(result3)
			if(len(result3)==0):
				print("--> The SS_RRSET ID tested for referential integrity: ",rrset_id)
				print("--> Referential integrity for SS_RRSET failed\n")
				logging.error("--> The SS_RRSET ID tested for referential integrity: %s",rrset_id)
				logging.error("--> Referential integrity for SS_RRSET failed\n")
				break
		else:
			break

	while True:
		exp_rrset_id=0
		exp_rr_type=0
		while True:
			random2 = random.randint(max_exp_rrset-table_count_arr[b][3], max_exp_rrset) 
			select_query_exp_rrset = "SELECT ID,RR_TYPE from "+str2+" WHERE ID=%s"
			cursor.execute(select_query_exp_rrset, (random2))
			result4 = cursor.fetchone()
			if(len(result4)!=0):
				exp_rrset_id = result4[0]
				exp_rr_type = result4[1]
				break

		join_table_exp_rrset = ""

		if(exp_rr_type == 48):
			join_table_exp_rrset = table_name_arr[7]
		elif(exp_rr_type == 43):
			join_table_exp_rrset = table_name_arr[9]
		elif(exp_rr_type == 52):
			join_table_exp_rrset = table_name_arr[11]
		elif(exp_rr_type == 2 or exp_rr_type == 6):
			join_table_exp_rrset = table_name_arr[5]

		join_query_exp_rrset = "SELECT "+table_name_arr[1]+".ID, "+table_name_arr[1]+".RR_TYPE from "+table_name_arr[1]+" inner join "+join_table_exp_rrset+" on "+table_name_arr[1]+".ID = "+join_table_exp_rrset+".SET_ID inner join "+table_name_arr[3]+" on "+table_name_arr[3]+".SET_ID = "+join_table_exp_rrset+".SET_ID where "+table_name_arr[1]+".ID=%s"
		join_query_exp_rrset_without_rrsig = "SELECT "+table_name_arr[1]+".ID, "+table_name_arr[1]+".RR_TYPE from "+table_name_arr[1]+" inner join "+join_table_exp_rrset+" on "+table_name_arr[1]+".ID = "+join_table_exp_rrset+".SET_ID where "+table_name_arr[1]+".ID=%s"
		logging.debug("--> The SS_EXP_RRSET ID tested for referential integrity: %s",exp_rrset_id)
		cursor.execute(join_query_exp_rrset, (exp_rrset_id))
		result5 = cursor.fetchall()
		logging.debug(result5)
		if(len(result5)==0):
			cursor.execute(join_query_exp_rrset_without_rrsig, (exp_rrset_id))
			result6 = cursor.fetchall()
			logging.debug(result6)
			if(len(result6)==0):
				print("--> The SS_EXP_RRSET ID tested for referential integrity: ",exp_rrset_id)
				print("--> Referential integrity for SS_EXP_RRSET failed\n")
				logging.error("--> The SS_EXP_RRSET ID tested for referential integrity: %s",exp_rrset_id)
				logging.error("--> Referential integrity for SS_EXP_RRSET failed\n")
				break
		else:
			break

	join_query_rrset_exp_rel = "SELECT "+table_name_arr[1]+".ID from "+table_name_arr[1]+" inner join "+table_name_arr[12]+" on "+table_name_arr[1]+".ID = "+table_name_arr[12]+".EXP_SET_ID where "+table_name_arr[1]+".ID=%s"
	logging.debug("--> The SS_RRSET_EXP_REL ID tested for referential integrity: %s",exp_rrset_id)
	cursor.execute(join_query_rrset_exp_rel, (exp_rrset_id))
	result7 = cursor.fetchall()
	logging.debug(result7)
	if(len(result7)==0):
		print("--> The SS_RRSET_EXP_REL ID tested for referential integrity: ",exp_rrset_id)
		print("--> Referential integrity for SS_RRSET_EXP_REL failed\n")
		logging.error("--> The SS_RRSET_EXP_REL ID tested for referential integrity: %s",exp_rrset_id)
		logging.error("--> Referential integrity for SS_RRSET_EXP_REL failed\n")

if __name__== "__main__":
	main()
