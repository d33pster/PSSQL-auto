#MENU

def menu():
    print("<-- Menu -->")
    
    print("\nFunctions:", end="\n")
    print("[111] Change active DataFrame")
    print("[222] Export current DataFrame into .csv")
    print("[333] Add more files")
    print("[444] Make custom output directory") #/Move output files
    
    print("\nQueries:", end="\n")
    print("[1] Show")
    print("[2] DataFrame Schema/Shape")
    print("[3] Drop a column")
    print("[4] Show top 10 entries")
    
    print("\n[p] Create a Pyspark Pipeline", end ="\n")
    
    print("\n[q] Custom Query", end ="\n")
    
    print("\n[x] Exit", end="\n")
    
    print("\n")
    menu_choice = input("--> ")
    return menu_choice
