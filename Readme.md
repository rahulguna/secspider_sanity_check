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
Run the create_sanity_check_table.sql file in the database for creation of SS_TABLE_SANITY_CHECK.

Step7:
Provide the database configuration details in the config ini. 
```
7.1 Encrypt the database password using encryption.py 
7.2 The passkey is hard coded.
7.3 Update the encrypted password in the config.ini file
7.4 Keep the config.ini in the same directory same the python program file
```

Step8:
Run the sanity script using the command:
```
python3 sanity_check.py
python3 sanity_check_monthly.py
```

Step9:
When the scripts are executed, the errors will be printed on the console. Please at the sanity_check.log file for debugging. 
