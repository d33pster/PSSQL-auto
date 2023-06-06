#!/usr/bin/env python

import os
import shutil
import platform
from simple_colors import *
import time


delDir1 = os.path.join(os.path.join(os.getcwd(),'main'), 'output-files')
delDir2 = os.path.join(os.path.join(os.getcwd(),'main'), 'usage-files')
delDir3 = os.path.join(os.path.join(os.getcwd(),'main'), 'pipelines')
delDir4 = os.path.join(os.path.join(os.getcwd(),'main'), 'pipeline-results')
    
if platform.system()=='Linux':
    if os.path.exists(delDir1):
        caller = os.system(f"rm -rf {delDir1}")
    if os.path.exists(delDir2):
        caller = os.system(f"rm -rf {delDir2}")
    if os.path.exists(delDir3):
        caller = os.system(f"rm -rf {delDir3}")
    if os.path.exists(delDir4):
        caller = os.system(f"rm -rf {delDir4}")
    caller = os.system("clear")
    print(yellow("Priviledged Content Cleaned!", 'blink'))
    time.sleep(5)
    os.system("clear")
elif platform.system=='Windows':
    if os.path.exists(delDir1):
        shutil.rmtree(delDir1)
    if os.path.exists(delDir2):    
        shutil.rmtree(delDir2)
    if os.path.exists(delDir3):    
        shutil.rmtree(delDir3)
    if os.path.exists(delDir4):    
        shutil.rmtree(delDir4)
    caller = os.system("cls")
    print(green("Priviledged Content Cleaned!", 'blink'))
    time.sleep(5)
    os.system("cls")
else:
    print(red("Nothing to Clean ~ Platform Not Supported"))