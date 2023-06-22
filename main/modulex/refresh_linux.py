from simple_colors import *
import os

directory_logo = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logo.sh')
def refresh_linux():
    caller = os.system("clear")
    caller = os.system(f"chmod +x {directory_logo}")
    caller = os.system(f"{directory_logo}")
    if caller!=0:
        print("error code Lx")
    print("\n", end="\n")