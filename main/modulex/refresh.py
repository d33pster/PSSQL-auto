import os
import subprocess
from simple_colors import *

def refresh():
    subprocess.run("cls", shell=True)
    with open(os.path.join(os.path.join(os.getcwd(), "main"), "logo.txt")) as logo:
        print(red(logo.read(), ['bright', 'bold', 'italic']))
    print("\n")