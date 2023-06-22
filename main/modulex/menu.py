#MENU
from simple_colors import *

def menu():
    print(magenta("\n<-- Menu -->", 'blink'))
    
    print(yellow("\nFunctions:"), end="\n")
    print(cyan("[111] Change active DataFrame"))
    print(cyan("[222] Export current DataFrame into .csv"))
    print(cyan("[333] Add more files"))
    print(cyan("[444] Make custom output directory")) #/Move output files
    
    print(yellow("\nQueries:"), end="\n")
    print(cyan("[1] Show"))
    print(cyan("[2] DataFrame Schema/Shape"))
    print(cyan("[3] Drop a column"))
    print(cyan("[4] Show top 10 entries"))
    
    print(yellow("\n[p] Create a Pyspark Pipeline"), end ="\n")
    
    print(yellow("\n[q] Custom Query"), end ="\n")
    
    print(magenta("\n[x] Exit", 'bright'), end="\n")
    
    print("\n")
    menu_choice = input("--> ")
    return menu_choice
