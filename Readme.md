Instrutions to run the sanity check:

Step1: 
Install Python 3.7.3

Step2:
Install mysql Ver 15.1 Distrib 10.3.15-MariaDB

Step3:
Install PyMySQL using this command: 
```
$python3 -m pip install PyMySQL
```

Step4:
Install configparser using this command: 
```
$python3 -m pip install configparser
```

Step5:
Install PyCryptodome using this command: 
```
$python3 -m pip install PyCryptodome
```

Step6:
Run the mysql/ddl/35_create_sanity_check_table.sql file in the database for creation of SS_TABLE_SANITY_CHECK.
```
6.1 Login to mysql database
6.2 Select the database using the command:
$use secspider;

6.3 Execute the SQL file using the command:
$source mysql/ddl/35_create_sanity_check_table.sql
```

Step7:
Provide the database configuration details in the scripts/sanity_check/config ini such as database name, username and password. 
```
7.1 Encrypt the database password using encryption.py 
$python3 encryption.py

Output:
ENCRYPTION
Enter Password to be encrypted...: sample
Ciphertext: f8JZyIkgvUp29j4ko0WbZk7JLQksJ5k5+M9c252j6aE=

DECRYPTION
Enter Ciphertext: f8JZyIkgvUp29j4ko0WbZk7JLQksJ5k5+M9c252j6aE=
Decrypted Password...: sample

7.2 The passkey is hard coded.
7.3 Update the encrypted password in the config.ini file
7.4 Keep the config.ini in the same directory same the python program file
```

Step8:
Run the sanity script using the command:
```
$python3 sanity_check.py
$python3 sanity_check_monthly.py
```

Step9:
When the scripts are executed, the errors will be printed on the console. Please look at the sanity_check.log file for more information. 
