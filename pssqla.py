#!/usr/bin/env python

#default packages
import os
import subprocess
import time
from simple_colors import *

from main.modulex.refresh import refresh

#Initial Checking and installations...
subprocess.run("cls", shell=True)
with open(os.path.join(os.path.join(os.getcwd(), "main"), "logo.txt")) as logo:
    print(red(logo.read(), 'bright'))
print("\n")
print(green("Initialising...", 'blink'))
time.sleep(2)
print("\n\n")
subprocess.run(['python.exe', '-m', 'pip', 'install', '--upgrade', 'pip'], shell=True)
subprocess.run(['pip', 'install', 'pyspark', 'matplotlib'], shell=True)
subprocess.run(['pip', 'install', '--upgrade', 'pyspark', 'matplotlib'], shell=True)
print("\n\n")
subprocess.run(['git', 'pull', 'https://github.com/d33pster/PSSQL-auto'], shell=True)
if not os.path.exists(os.path.join(os.path.join(os.getcwd(),"main"), "usage-files")):
    os.mkdir(os.path.join(os.path.join(os.getcwd(),"main"), "usage-files"))
if not os.path.exists(os.path.join(os.path.join(os.getcwd(),"main"), "output-files")):
    os.mkdir(os.path.join(os.path.join(os.getcwd(),"main"), "output-files"))

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

#creating filer for menu
def filer(DataFrame):
    file_name = input("filename (W/O extention): ")
    filepath = os.path.join(os.path.join(os.path.join(os.getcwd(), "main"),"output-files"), file_name+".csv")
    if os.path.exists(filepath):
        print(f"\nA File named {file_name} already exists..\nPress 0 to overwrite :: Press 1 to create new:")
        filer_ch = int(input(":: "))
        if filer_ch==1:
            for i in range(100):
                if i==0:
                    continue
                suffix = str(i)
                filepath_tmp = os.path.join(os.path.join(os.path.join(os.getcwd(), "main"),"output-files"), file_name+"["+suffix+"].csv")
                if not os.path.exists(filepath_tmp):
                    DataFrame.toPandas().to_csv(filepath_tmp)
                    print(f"\nFile Exported Succesfully to {filepath_tmp}")
                    code_choke = input("\n~~Press Enter to Continue~~")
                    break
        elif filer_ch==0:
            DataFrame.toPandas().to_csv(filepath)
            print(f"\nFile Exported Succesfully to {filepath}")
            code_choke = input("\n~~Press Enter to Continue~~")
        else:
            print("Invalid Input!")
            print("RECONFIGURING FROM LAST CHECKPOINT....")
            time.sleep(4)
            filer(DataFrame)
    else:
        DataFrame.toPandas().to_csv(filepath)
        print(f"\nFile Exported Succesfully to {filepath}")
        code_choke = input("\n~~Press Enter to Continue~~")

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
    print("\n")
    return menu_df_active
    
active = menu_startup()

def menu_load():
        
    choice = menu()
    global active
    #menu-driver
    refresh()
    if choice==111:
        active = menu_startup()
        print("active DF changed!")
        time.sleep(2.6)
        refresh()
        print(green(f"active --> {active}", 'bink'))
        menu_load()
    elif choice==1: #show
        for item in DataFrames:
            if item["Name"] == active:
                print(f"DataFrame {active}:")
                print(f"{item['df'].show()}")
                code_choke = input("~Press Enter to continue~")
                refresh()
                print(green(f"active --> {active}", 'bink'))
                menu_load()
    elif choice==2: #del column
        dColumn = input("column to delete: ")
        for item in DataFrames:
            if item["Name"]==active:
                item["df"] = item["df"].drop(dColumn)
                print("Resulting DataFrame \\-/ ")
                print(item["df"].show())
                code_choke = input("~Press Enter to continue~")
                refresh()
                print(green(f"active --> {active}", 'bink'))
                menu_load()
    elif choice==3: #top10
        for item in DataFrames:
            if item["Name"]==active:
                query = item["df"].select("*").limit(10)
                query.show()
                code_choke = input("~Press Enter to Continue~")
                refresh()
                print(green(f"active --> {active}", 'bink'))
                menu_load()
    elif choice==222: #export to csv
        for item in DataFrames:
            if item["Name"]==active:
                filer(item["df"])
                refresh()
                print(green(f"active --> {active}", 'blink'))
                menu_load()
    elif choice==99:
        return
    else:
        print("Error!")
        print("RECONFIGURING...")
        time.sleep(3)
        print(green(f"active --> {active}", 'bink'))
        menu_load()
    return

menu_load()

#concluder
def concluder(): 
    print(cyan("\nCleaning Up...", 'blink'))
    time.sleep(2)
    for item in Filenames_lo:
        os.remove(item['Location'])
        print(red(f"Deleted {item['Name']} from local directory {item['Location']}."))
    time.sleep(3)
    refresh()
    print(cyan("\nQuiting..."))
    time.sleep(2)
    os.system("cls")
    
concluder()