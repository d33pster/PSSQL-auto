#!/usr/bin/env python

#default packages
import os
import time
from simple_colors import *

from modulex.refresh_linux import refresh_linux

#Initial Checking and installations...
refresh_linux()
print(red("Initialising...", 'blink'))
time.sleep(2)
print("\n")
os.system('pip install --upgrade pip')
os.system('pip install pyspark matplotlib')
os.system('pip install --upgrade pyspark matplotlib')
print("\n\n")
#subprocess.run(['git', 'pull', 'https://github.com/d33pster/PSSQL-auto'], shell=True)
if not os.path.exists(os.path.join(os.path.join(os.getcwd(),"main"), "usage-files")):
    os.mkdir(os.path.join(os.path.join(os.getcwd(),"main"), "usage-files"))
if not os.path.exists(os.path.join(os.path.join(os.getcwd(),"main"), "output-files")):
    os.mkdir(os.path.join(os.path.join(os.getcwd(),"main"), "output-files"))

#Import installed packages...
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc , col, max, struct
import matplotlib.pyplot as plt

#Input required processing data
refresh_linux()

#pyspark session
spark = SparkSession.builder.appName("PSSQLA").getOrCreate()
refresh_linux()

#storing file locations
Filenames = []
Filenames_lo = []
DataFrames = []

def readFile(current_filecount) -> int:
    nf = int(input(cyan("Number of csv files to be imported\n:: ")))
    refresh_linux()
    count = nf + current_filecount
    for i in range(nf):
        print(cyan(f"File [{i+1+current_filecount}] absolute path: "), end=" ")
        file = input()
        #filename = file.split("\\")[-1]
        filename_wx = os.path.splitext(file)[0]
        filename_wx = filename_wx.split("/")[-1]
        temp = {"Name": filename_wx, "Location": file}
        Filenames.append(temp)
    #Localising Filenames
    a=0
    refresh_linux()
    print(yellow("Localising ...", 'blink'))
    for item in Filenames:
        if a<current_filecount:
            a=a+1
            continue
        caller = os.system(f"cp {item['Location']} {os.path.join(os.path.join(os.path.join(os.getcwd(), 'main'),'usage-files'), item['Location'].split('/')[-1])}")
        time.sleep(1.5)
        temp = {"Name": item["Name"], "Location": os.path.join(os.path.join(os.path.join(os.getcwd(), "main"),"usage-files"), item["Location"].split("/")[-1])}
        Filenames_lo.append(temp)
    refresh_linux()
    #Creating data frames...
    print(yellow("Processing...", 'blink'))
    time.sleep(1)
    a=0
    for itemz in Filenames_lo:
        if a<current_filecount:
            a=a+1
            continue
        filename = itemz['Location'].split('/')[-1]
        print(yellow(f"Creating dataframe {itemz['Name']} from {filename} ..."))
        df = spark.read.format('csv').option('inferSchema', True).option('header', True).load(itemz["Location"])
        time.sleep(1)
        print(yellow(f"Dropping Null rows..."))
        df = df.na.drop()
        time.sleep(1)
        temp = {"Name": itemz["Name"], "df": df}
        DataFrames.append(temp)
    print(green("\nDataframes created and stored Successfully.."))
    time.sleep(2)
    return count

file_count = readFile(0) #init
output_dir = None
#creating filer for menu
def filer(DataFrame, output_direc):
    global output_dir
    output_dir = output_direc
    file_name = input(cyan("filename (W/O extention): "))
    if output_dir==None:
        output_dir = os.path.join(os.path.join(os.getcwd(), "main"),"output-files")
    filepath = os.path.join(output_dir, file_name+".csv")
    if os.path.exists(filepath):
        refresh_linux()
        print(red(f"A File named {file_name} already exists..", 'blink')+cyan("\nPress 0 to overwrite :: Press 1 to create new:"))
        filer_ch = int(input(":: "))
        if filer_ch==1:
            for i in range(100):
                if i==0:
                    continue
                suffix = str(i)
                filepath_tmp = os.path.join(output_dir, file_name+"["+suffix+"].csv")
                if not os.path.exists(filepath_tmp):
                    DataFrame.toPandas().to_csv(filepath_tmp)
                    print(green(f"\nFile Exported Succesfully to {filepath_tmp}"))
                    code_choke = input(cyan("\n~~Press Enter to Continue~~", 'blink'))
                    break
        elif filer_ch==0:
            DataFrame.toPandas().to_csv(filepath)
            print(green(f"\nFile Exported Succesfully to {filepath}"))
            code_choke = input(cyan("\n~~Press Enter to Continue~~", 'blink'))
        else:
            print(red("Invalid Input!", 'blink'))
            print(yellow("RECONFIGURING FROM LAST CHECKPOINT....", 'blink'))
            time.sleep(4)
            filer(DataFrame, output_dir)
    else:
        DataFrame.toPandas().to_csv(filepath)
        print(green(f"\nFile Exported Succesfully to {filepath}"))
        code_choke = input(cyan("\n~~Press Enter to Continue~~", 'blink'))

#Creating menu
from modulex.menu import menu
def menu_startup():
    refresh_linux()
    print(green("Following DataFrames are available for Processing ..."))
    i=0
    for item in DataFrames:
        print(f"[DataFrame {i+1}]" + green(f" {item['Name']}"))
        i=i+1
    print("\n")
    menu_df_active = input(cyan("Use (name)--> ", 'blink'))   #Active DF
    print("\n")
    refresh_linux()
    return menu_df_active
    
active = menu_startup()

def menu_load():
    global active, file_count, output_dir
    print(green(f"active --> {active}", 'blink'))
    choice = menu()
    #menu-driver
    refresh_linux()
    if choice==111:
        active = menu_startup()
        print(green("active DF changed!", 'blink'))
        time.sleep(2.6)
        refresh_linux()
        menu_load()
    elif choice==1: #show
        for item in DataFrames:
            if item["Name"] == active:
                print(magenta(f"DataFrame {active}:"))
                print(item['df'].show())
                code_choke = input(cyan("~Press Enter to continue~", 'blink'))
                refresh_linux()
                menu_load()
    elif choice==2: #del column
        dColumn = input(cyan("column to delete: "))
        for item in DataFrames:
            if item["Name"]==active:
                item["df"] = item["df"].drop(dColumn)
                print(green("<-- Resulting DataFrame -->", 'blink'))
                print(item["df"].show())
                code_choke = input(cyan("~Press Enter to continue~", 'blink'))
                refresh_linux()
                menu_load()
    elif choice==3: #top10
        for item in DataFrames:
            if item["Name"]==active:
                query = item["df"].select("*").limit(10)
                query.show()
                code_choke = input(cyan("~Press Enter to Continue~", 'blink'))
                refresh_linux()
                menu_load()
    elif choice==222: #export to csv
        for item in DataFrames:
            if item["Name"]==active:
                filer(item["df"], output_dir)
                refresh_linux()
                menu_load()
    elif choice==333: #add more files
        file_count = readFile(file_count)
        refresh_linux()
        active = menu_startup()
        menu_load()
    elif choice==444: #customize output dir
        new_output_path = input(cyan("New Output Path: "))
        output_dir = new_output_path
        print(green(f"\nOutput Directory Changed to -> {output_dir}"))
        print(red("All further outputs will be saved Here!", ['bright', 'blink']))
        time.sleep(4.5)
        refresh_linux()
        menu_load()
    elif choice==99:
        return
    else:
        print(red("Error!", 'blink'))
        print(yellow("RECONFIGURING..."))
        time.sleep(3)
        menu_load()
    return

menu_load()

#concluder
def concluder(): 
    print(yellow("\nCleaning Up...", 'blink'))
    time.sleep(2)
    for item in Filenames_lo:
        os.remove(item['Location'])
        print(red(f"Deleted {item['Name']} from local directory {item['Location']}.", 'bright'))
    time.sleep(3)
    refresh_linux()
    print(cyan("\nQuiting..."))
    time.sleep(2)
    os.system("clear")
    
concluder()