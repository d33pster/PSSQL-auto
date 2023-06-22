#!/usr/bin/env python

import platform, os, subprocess, time

if platform.system()=='Linux':
    print("Installing....")
    time.sleep(2)
    os.system('pip install --upgrade pip')
    os.system('pip install pyspark matplotlib findspark alive-progress simple-colors')
    os.system('pip install --upgrade pyspark matplotlib findspark alive-progress simple-colors')
    os.system('clear')
    print("Done!")
    time.sleep(5)
    os.system("chmod +x ./pssqla.py")
    os.system("./pssqla.py")
elif platform.system()=='Windows':
    print("Installing....")
    time.sleep(2)
    caller = subprocess.run(['python.exe', '-m', 'pip', 'install', '--upgrade', 'pip'], shell=True)
    caller = subprocess.run(['pip', 'install', 'pyspark', 'matplotlib', 'findspark', 'alive-progress', 'simple-colors'], shell=True)
    caller = subprocess.run(['pip', 'install', '--upgrade', 'pyspark', 'matplotlib', 'findspark', 'alive-progress', 'simple-colors'], shell=True)
    os.system("cls")
    print("Done!")
    time.sleep(5)
    caller = subprocess.run(['pssqla.py'], shell=True)
elif platform.system()=='Darwin':
    os.system("clear")
    print("macOs detected.")
    time.sleep(2)
    print("Installing ...")
    time.sleep(2)
    os.system("pip install --upgrade pip")
    os.system("pip install pyspark matplotlib findspark alive-progress simple-colors")
    os.system("pip install --upgrade pyspark matplotlib findspark alive-progress simple-colors")
    os.system("clear")
    print("done.")
    time.sleep(4)
    os.system("clear")
    os.system(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pssqla.py'))
else:
    print("Not supported in this Platform")