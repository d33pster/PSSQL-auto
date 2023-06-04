#!/usr/bin/env python

import platform
import subprocess
import os
import time
from simple_colors import *

PLATFORM = platform.system()
wDir = os.path.join(os.path.join(os.getcwd(), 'main'), 'pssqla_windows.py')

if PLATFORM=='Windows':
    caller = os.system("cls")
    print(green("Platform Detected: Windows", 'blink'))
    time.sleep(4)
    caller = subprocess.run(['python', wDir], shell=True)
elif PLATFORM=='Linux':
    caller = os.system("clear")
    print(green("Platform Detected: Linux", 'blink'))
    time.sleep(4)
    subprocess.call(['chmod','+x','./main/pssqla_linux.py'])
    subprocess.call(['./main/pssqla_linux.py'])
else:
    print("This Platform is not Supported yet!")