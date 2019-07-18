#!/usr/bin/python
import pymysql as mariadb
import pymysql.cursors
import configparser
import datetime
from datetime import timezone
import sys
import random 

#configuration settings
config = configparser.ConfigParser()
config.read('config.ini')

print("Secspider Sanity Check\n")

#get password from user
password  = input("Enter your password to connect database:\n")
if(password == ""):
	print("You have not entered any password")
	sys.exit(0)

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

#global variables
curr_run=0;

table_name_arr = ['SS_ZONE']

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
	while(index2 <= 18): 
		calculate_rows_inserted_stats_table(index2, table_name_arr[index2])
		index2 += 1

	#Calculate of expected behaviour and sanity check for SS_RRSET table
	range_arr=calculate_expected_behaviour(0, table_name_arr[0])
	check_sanity(0, table_name_arr[0], 1, table_name_arr[1], range_arr)
	
	#Sanity check for SS_RRSIG, SS_EXP_RRSIG, SS_RR, SS_EXP_RR, SS_DNSKEY, SS_EXP_DNSKEY, SS_DS, SS_EXP_DS, SS_TLSA, SS_EXP_TLSA
	index3=2
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
	
	print("[Table_Name, #Rows in current run, #Rows in previous run, Rows inserted]")
	for item in table_count_arr:
		print(item)
	print("\n")

	#Check referential integrity
	check_referential_integrity(0, table_name_arr[0], 1, table_name_arr[1]);

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

	print("--> The data for the lastest run is inserted into sanity check table\n")

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
		print("--> There are no previous data in the sanity check table. Please run the sanity check again after the next run\n")
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

def calculate_rows_inserted(a, str1, b, str2):
	if table_count_arr[b][0]==str2:
		table_count_arr[b][3]=table_count_arr[b][1]-table_count_arr[b][2]
	if table_count_arr[a][0]==str1:
		result=table_count_arr[a][2]-table_count_arr[b][3]
		table_count_arr[a][3]=table_count_arr[a][1]-result

def calculate_rows_inserted_stats_table(a, str1):
	if table_count_arr[a][0]==str1:
		table_count_arr[a][3]=table_count_arr[a][1]-table_count_arr[a][2]

def calculate_expected_behaviour(a, str1):

	today = datetime.date.today()
	one_month = datetime.timedelta(days=31)
	two_month = datetime.timedelta(days=62)
	three_month = datetime.timedelta(days=92)
	#one_month_back = today - one_month
	#two_month_back = today - two_month
	#three_month_back = today - three_month
	
	last_month_total_count = 0
	two_month_total_count = 0
	three_month_total_count = 0
	last_month_avg_count = 0
	two_month_avg_count = 0
	three_month_avg_count = 0

	one_month_back = datetime.date(2018, 10, 21)
	two_month_back = datetime.date(2018, 7, 21)
	three_month_back = datetime.date(2018, 4, 21)

	select_query_zone_count = "SELECT COUNT(ID) AS COUNT FROM "+str1+" WHERE YEAR(FROM_UNIXTIME(FIRST_SEEN)) = %s AND MONTH(FROM_UNIXTIME(FIRST_SEEN)) = %s"
	cursor.execute(select_query_zone_count, (one_month_back.year,one_month_back.month))
	for COUNT in cursor:
		last_month_total_count = COUNT[0]
	cursor.execute(select_query_zone_count, (two_month_back.year,two_month_back.month))
	for COUNT in cursor:
		two_month_total_count = COUNT[0]
	cursor.execute(select_query_zone_count, (three_month_back.year,three_month_back.month))
	for COUNT in cursor:
		three_month_total_count = COUNT[0]
	
	temp = last_month_total_count + two_month_total_count + three_month_total_count
	if(temp != 0):
		avg_total_count = int(temp/3)
	else:
		print("--> No sufficient data available to calculate for the expected behaviour for "+str1+" table\n")
		return []
	
	zone_count = table_count_arr[18][2]
	select_query_zone_count = "SELECT AVG(SUM) as AVG from (select COUNT(ID) AS SUM from "+str1+" WHERE YEAR(FROM_UNIXTIME(FIRST_SEEN)) = %s AND MONTH(FROM_UNIXTIME(FIRST_SEEN)) = %s group by ZONE_ID) MYTABLE"
	cursor.execute(select_query_avg_count, (one_month_back.year,one_month_back.month))
	for AVG in cursor:
	    last_month_avg_count = AVG[0]
	cursor.execute(select_query_avg_count, (two_month_back.year,two_month_back.month))
	for AVG in cursor:
	    two_month_avg_count = AVG[0]
	cursor.execute(select_query_avg_count, (three_month_back.year,three_month_back.month))
	for AVG in cursor:
		three_month_avg_count = AVG[0]

	avg_count_per_zone = int((last_month_avg_count + two_month_avg_count + three_month_avg_count)/3)
	
	if((avg_total_count - 50) < 0):
		ZONE_FROM = 0
	else:
		ZONE_FROM = avg_total_count - 50

	if((avg_count_per_zone - 3) < 0):
		AVG_FROM = 0
	else:
		AVG_FROM = avg_count_per_zone - 3

	ZONE_TO = avg_total_count + 50
	AVG_TO = avg_count_per_zone + 3

	expected_range_arr = [ZONE_FROM, ZONE_TO, AVG_FROM, AVG_TO]
	print("--> Calculated expected range for "+str1+" : ",expected_range_arr)
	return expected_range_arr

def check_sanity(a, str1, b, str2, range_arr):
	if(table_count_arr[b][3] < 0):
		print("--> Rows have been dropped from "+str2+" table\n")

	if(len(range_arr) == 0):
		print("--> No sufficient data perform the sanity check for "+str1+" table\n")
		return
	
	print("--> Sanity Check for "+str1+" table")

	#year = datetime.date.today().year
	#month = datetime.date.today().month
	year = 2018
	month = 10

	TOTAL_ZONE_COUNT = 0

	select_query_zone_count = "SELECT ZONE_ID FROM "+str1+" WHERE YEAR(FROM_UNIXTIME(FIRST_SEEN)) = %s AND MONTH(FROM_UNIXTIME(FIRST_SEEN)) = %s group by zone_id"
	cursor.execute(select_query_zone_count, (year,month))
	TOTAL_ZONE_COUNT = len(cursor.fetchall())
	print("--> Total number of zones inserted data in "+str1+" table: ",TOTAL_ZONE_COUNT)
	
	select_query_avg_count = "SELECT AVG(SUM) as AVG from (select COUNT(ID) AS SUM from "+str1+" WHERE YEAR(FROM_UNIXTIME(FIRST_SEEN)) = %s AND MONTH(FROM_UNIXTIME(FIRST_SEEN)) = %s group by ZONE_ID) MYTABLE"
	cursor.execute(select_query_avg_count, (year,month))
	for AVG in cursor:
	    print("--> Average Rows inserted per zone in "+str1+" table: ",AVG[0])

	ZONE_FROM = range_arr[0]
	ZONE_TO = range_arr[1]
	AVG_FROM = range_arr[2]
	AVG_TO = range_arr[3]

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
				return
			else:
				status=1
			index +=2

	if TOTAL_ZONE_COUNT<ZONE_FROM and TOTAL_ZONE_COUNT>ZONE_TO:
		print("--> The sanity check for "+str1+": FAILED - Condition 2\n")
		return

	if AVG[0]<AVG_FROM and AVG[0]>AVG_TO:
		print("--> The sanity check for "+str1+": FAILED - Condition 3\n")
		return

	if status==1:
		print("--> The sanity check for "+str1+": PASSED\n")
	else:
		print("--> The sanity check for "+str1+": FAILED - Condition 4\n")

def check_sanity_set_tables(a, str1, b, str2):
	status=1
	if(table_count_arr[b][3] < 0):
		status=0
		print("--> Rows have been dropped from "+str2+" table\n")
	if(table_count_arr[a][3] < 0):
		status=0
		print("--> Rows have been dropped from "+str1+" table\n")
	
	if(table_count_arr[a][3] > 0):
		if(table_count_arr[0][3] > 0):
			pass
		else:
			status=0
			print("--> "+str1+" tables has values inserted in it, but "+table_count_arr[0][0]+" does not have values inserted\n")

	if status==1:
		print("--> The sanity check for "+str1+": PASSED\n")
	else:
		print("--> The sanity check for "+str1+": FAILED\n")

def	check_stats_tables(a, str1):
	if table_count_arr[a][0]==str1:
		if(table_count_arr[a][3] < 0):
			print("--> Rows have been dropped from "+str1+" table\n")
		elif(table_count_arr[a][3] == 0):
			print("--> No new insertions in "+str1+" table\n")
	
	today = datetime.date.today()
	one_month = datetime.timedelta(days=1)
	two_month = datetime.timedelta(days=2)
	three_month = datetime.timedelta(days=3)
	one_month_back = today - one_month
	two_month_back = today - two_month
	three_month_back = today - three_month
	
	last_month_total_count = 0
	two_month_total_count = 0
	three_month_total_count = 0
	current_total_count = 0

	#today = datetime.date(2018, 10, 21)
	#one_month_back = datetime.date(2018, 10, 21)
	#two_month_back = datetime.date(2018, 7, 21)
	#three_month_back = datetime.date(2018, 4, 21)

	select_query_zone_count = "SELECT COUNT(ID) FROM "+str1+" WHERE YEAR(FROM_UNIXTIME(SEEN)) = %s AND MONTH(FROM_UNIXTIME(SEEN)) = %s"
	
	cursor.execute(select_query_zone_count, (one_month_back.year,one_month_back.month))
	for COUNT in cursor:
	    last_month_total_count = COUNT[0]
	cursor.execute(select_query_zone_count, (two_month_back.year,two_month_back.month))
	for COUNT in cursor:
	    two_month_total_count = COUNT[0]
	cursor.execute(select_query_zone_count, (three_month_back.year,three_month_back.month))
	for COUNT in cursor:
	    three_month_total_count = COUNT[0]

	temp = last_month_total_count + two_month_total_count + three_month_total_count
	if(temp != 0):
		avg_total_count = int(temp/3)
	else:
		print("--> No sufficient data available to calculate for the expected behaviour for "+str1+" table\n")
		return

	cursor.execute(select_query_zone_count, (today.year,today.month))
	for COUNT in cursor:
	    current_total_count = COUNT[0]

	if((avg_total_count - 100) < 0):
		RANGE_FROM = 0
	else:
		RANGE_FROM = avg_total_count - 100

	RANGE_TO = avg_total_count + 100

	if RANGE_FROM <= current_total_count <= RANGE_TO:
		print("--> The sanity check for "+str1+": PASSED")
		print("--> The total count of values inserted in "+str1+" is "+str(current_total_count)+" which is not in range of "+str(RANGE_FROM)+" and "+str(RANGE_TO)+"\n")
	else:
		print("--> The sanity check for "+str1+": FAILED")
		print("--> The total count of values inserted in "+str1+" is "+str(current_total_count)+" which is not in range of "+str(RANGE_FROM)+" and "+str(RANGE_TO)+"\n")

def check_RRSET_EXP_REL(a, str1):
	status=1
	if(table_count_arr[a][3] < 0):
			status=0
			print("--> Rows have been dropped from "+str1+" table\n")

	year=datetime.datetime.now().strftime('%y')
	cursor.execute("SELECT COUNT(ID) from SS_RRSET_EXP_REL where EXP_SET_ID is not null and YEAR(FROM_UNIXTIME(SEEN)) = %s",(year))
	for ID in cursor:
		if(ID[0]==0):
			status=0
			print("--> EXP_SET_ID column in "+str1+" is not being set\n")

	if(table_count_arr[a][3] > 0):
		if(table_count_arr[0][3] > 0):
			pass
		else:
			status=0
			print("--> "+str1+" tables has values inserted in it, but "+table_count_arr[0][0]+" does not have values inserted\n")

	if status==1:
		print("--> The sanity check for "+str1+": PASSED\n")
	else:
		print("--> The sanity check for "+str1+": FAILED\n")

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

	rrset_id=0
	rrset_rr_type=0
	while True:
		random1 = random.randint(max_rrset-table_count_arr[a][3], max_rrset) 
		select_query_rrset = "SELECT ID,RR_TYPE from "+str1+" WHERE ID=%s"
		cursor.execute(select_query_rrset, (random1))
		result1 = cursor.fetchone()
		if(len(result1)!=0):
			for item in result1:
				rrset_id = item[0]
				rrset_rr_type = item[1]
			break

	exp_rrset_id=0
	exp_rr_type=0
	while True:
		random2 = random.randint(max_exp_rrset-table_count_arr[b][3], max_exp_rrset) 
		select_query_exp_rrset = "SELECT ID,RR_TYPE from "+str2+" WHERE ID=%s"
		cursor.execute(select_query_exp_rrset, (random2))
		result2 = cursor.fetchone()
		if(len(result2)!=0):
			for item in result2:
				exp_rrset_id = item[0]
				exp_rr_type = item[1]
			break
	
	join_table_rrset = ""
	
	if(rrset_rr_type == 48):
		join_table_rrset = table_name_arr[6]
	elif(rrset_rr_type == 43):
		join_table_rrset = table_name_arr[8]
	elif(rrset_rr_type == 52):
		join_table_rrset = table_name_arr[10]
	elif(rrset_rr_type == 2 || rrset_rr_type == 6):
		join_table_rrset = table_name_arr[4]

	join_table_exp_rrset = ""

	if(exp_rr_type == 48):
		join_table_exp_rrset = table_name_arr[7]
	elif(exp_rr_type == 43):
		join_table_exp_rrset = table_name_arr[9]
	elif(exp_rr_type == 52):
		join_table_exp_rrset = table_name_arr[11]
	elif(exp_rr_type == 2 || exp_rr_type == 6):
		join_table_exp_rrset = table_name_arr[5]

	join_query_rrset = "SELECT "+table_name_arr[0]+".ID, "+table_name_arr[0]+".RR_TYPE from "+table_name_arr[0]+" inner join "+join_table_rrset+" on "+table_name_arr[0]+".ID = "+join_table_rrset+".SET_ID inner join "+table_name_arr[2]+" on "+table_name_arr[2]+".SET_ID = "+join_table_rrset+".SET_ID where "+table_name_arr[0]+".ID=%s"
	cursor.execute(join_query_rrset, (rrset_id))
	result3 = cursor.fetchall()
		if(len(result3)==0):
			print("Referential integrity for SS_RRSET failed")

	join_query_exp_rrset = "SELECT "+table_name_arr[1]+".ID, "+table_name_arr[1]+".RR_TYPE from "+table_name_arr[1]+" inner join "+join_table_exp_rrset+" on "+table_name_arr[1]+".ID = "+join_table_exp_rrset+".SET_ID inner join "+table_name_arr[3]+" on "+table_name_arr[3]+".SET_ID = "+join_table_exp_rrset+".SET_ID where "+table_name_arr[1]+".ID=%s"
	cursor.execute(join_query_exp_rrset, (exp_rrset_id))
	result4 = cursor.fetchall()
		if(len(result4)==0):
			print("Referential integrity for SS_EXP_RRSET failed")

if __name__== "__main__":
	main()
