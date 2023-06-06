# PSSQL-auto v3.2 Insructions
This Project contains a python app for data analysis using Pyspark SQL
# For First Use
1. git clone the repo
2. go to PSSQL-auto directory in terminal/cmd
3. type prereq.py in cmd and hit enter OR type chmod +x ./prereq.py in Linux terminal hit enter and then type ./prereq.py and hit enter
## PREREQ
1. need to uninstall all python dists below 3 (keep only the latest)
2. need Java (installer added) with JAVA_HOME variable pointing to the installation folder without the /bin folder
## Run
ps. Eliminated the need to run more than 2 line of different commands in windows and linux
1. for Windows simply go to cloned directory (PSSQL-auto) and run "python pssqla.py" in CMD
2. for Linux go to cloned directory and run the following commands ~
    ~ sudo chmod +x ./pssqla.py
    ~ sudo ./pssqla.py
## clean
1. as u may notice some files and folders are not accessible from the main folder
2. to delete them use clean.py (in linux give perms using chmod, in windows type python before script path)
3. to copy or move use administrator privilage (in linux use sudo)
## Custom Queries
1. you can now add custom Pyspark SQL queries and save and export the resulting dataframes ;)