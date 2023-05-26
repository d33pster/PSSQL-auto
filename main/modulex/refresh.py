import os
import subprocess

def refresh():
    subprocess.run("cls", shell=True)
    with open(os.path.join(os.path.join(os.getcwd(), "main"), "logo.txt")) as logo:
        print(logo.read())
    print("\n")