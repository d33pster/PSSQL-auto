#!/usr/bin/env python

#default packages
import os
import subprocess
import time
from main.modulex.refresh import refresh

#Initial Checking and installations...
subprocess.run("cls", shell=True)
with open(os.path.join(os.path.join(os.getcwd(), "main"), "logo.txt")) as logo:
    print(logo.read())
print("\n")
print("Initialising...")
time.sleep(2)
print("\n\n")
subprocess.run(['python.exe', '-m', 'pip', 'install', '--upgrade', 'pip'], shell=True)
subprocess.run(['pip', 'install', 'pyspark', 'matplotlib'], shell=True)
subprocess.run(['pip', 'install', '--upgrade', 'pyspark', 'matplotlib'], shell=True)
print("\n\n")
subprocess.run(['git', 'pull', 'https://github.com/d33pster/PSSQL-auto'], shell=True)
if not os.path.exists(os.path.join(os.path.join(os.getcwd(),"main"), "usage-files")):
    os.mkdir(os.path.join(os.path.join(os.getcwd(),"main"), "usage-files"))

#Import installed packages...
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc , col, max, struct
import matplotlib.pyplot as plt

#Input required processing data
refresh()

#pyspark session
spark = SparkSession.builder.appName("SparkApp").getOrCreate()
refresh()

#storing file locations
nf = int(input("Number of csv files to be imported\n:: "))
Filenames = []
for i in range(nf):
    print(f"File [{i+1}] absolute path: ")
    file = input()
    filename = file.split("\\")[-1]
    #subprocess.run(['copy', file, os.path.join(os.path.join(os.path.join(os.getcwd(), "main"),"usage-files"), filename)], shell=True)
    #geting filename without extention
    filename_wx = os.path.splitext(file)[0]
    filename_wx = filename_wx.split("\\")[-1]
    temp = {"Name": filename_wx, "Location": file}
    Filenames.append(temp)
Filenames_lo = []
for item in Filenames:
    print("\nLocalising ...")
    subprocess.run(['copy', item["Location"], os.path.join(os.path.join(os.path.join(os.getcwd(), "main"),"usage-files"), item["Location"].split("\\")[-1])], shell=True)
    temp = {"Name": item["Name"], "Location": os.path.join(os.path.join(os.path.join(os.getcwd(), "main"),"usage-files"), item["Location"].split("\\")[-1])}
    Filenames_lo.append(temp)
refresh()




#Creating data frames...
DataFrames = []
print("Processing...")
time.sleep(1)

for itemz in Filenames_lo:
    filename = itemz['Location'].split('\\')[-1]
    print(f"Creating dataframe {itemz['Name']} from {filename} ...")
    df = spark.read.format('csv').option('inferSchema', True).option('header', True).load(itemz["Location"])
    print(f"Dropping Null rows...")
    df = df.na.drop()
    temp = {"Name": itemz["Name"], "df": df}
    DataFrames.append(temp)

print("Dataframes created and stored Successfully..")

#Creating menu
from main.modulex.menu import menu
def menu_startup():
    refresh()
    print("Following DataFrames are available for Processing ...")
    i=0
    for item in DataFrames:
        print(f"[DataFrame {i+1}] {item['Name']}")
        i=i+1
    print("\n")
    menu_df_active = input("Use (name)--> ")   #Active DF
    return menu_df_active
    
active = menu_startup()

def menu_load():
        
    choice = menu()
    global active
    #menu-driver
    refresh()
    if choice==1:
        active = menu_startup()
        print("active DF changed!")
        time.sleep(2.6)
        refresh()
        print(f"active --> {active}")
        menu_load()
    #elif choice==0:
    #    print(f"active --> {active}")
    #    print("Loading...")
    #    time.sleep(4)
    #    refresh()
    #    menu_load()
    elif choice==2:
        for item in DataFrames:
            if not item["Name"] == active:
                continue
            else:
                print(f"DataFrame {active}:")
                print(f"{item['df'].show()}")
                code_choke = input("~Press Enter to continue~")
                refresh()
                print(f"active --> {active}")
                menu_load()
    elif choice==3:
        dColumn = input("column to delete: ")
        for item in DataFrames:
            if not item["Name"]==active:
                continue
            else:
                item["df"] = item["df"].drop(dColumn)
                print("Resulting DataFrame \\-/ ")
                print(item["df"].show())
                code_choke = input("~Press Enter to continue~")
                refresh()
                print(f"active --> {active}")
                menu_load()
    elif choice==4:
        for item in DataFrames:
            if not item["Name"]==active:
                continue
            else:
                query = item["df"].select("*").limit(10)
                query.show()
                code_choke = input("~Press Enter to Continue~")
                refresh()
                print(f"active --> {active}")
                menu_load()
    elif choice==99:
        return
    else:
        print("Error!")
        print("RECONFIGURING...")
        time.sleep(3)
        print(f"active --> {active}")
        menu_load()
    return

menu_load()


