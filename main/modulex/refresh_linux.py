from simple_colors import *
import os

def refresh_linux():
    caller = os.system("clear")
    caller = os.system("chmod +x ./main/logo.sh")
    caller = os.system("./main/logo.sh")
    if caller!=0:
        print("error code Lx")
    print("\n", end="\n")