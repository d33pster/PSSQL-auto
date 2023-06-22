#!/usr/bin/env python


# ~~~~~~~~~~~~~~~~~~ Next Update ~~~~~~~~~~~~~~~~~~~
#
#
#
#
#
#

import platform
import subprocess
import os
import time
from simple_colors import *

PLATFORM = platform.system()
wDir = os.path.join(os.path.join(os.getcwd(), 'main'), 'pssqla_windows.py')
macDIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'main', 'pssqla_mac')

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
elif PLATFORM=='Darwin':
    os.system("clear")
    print(yellow("Platform Detected: macOs", 'blink'))
    time.sleep(4)
    os.system(f"chmod +x {macDIR}")
    os.system(f"{macDIR}")
else:
    print("This Platform is not Supported yet!")