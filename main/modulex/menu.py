#MENU

def menu():
    print("<-- Menu -->")
    
    print("\nFunctions:")
    print("[111] Change active DataFrame")
    print("[222] Export current DataFrame into .csv")
    #print("[333] Add more files")
    #change output-directory
    
    print("\nQueries:")
    print("[1] Show")
    print("[2] Drop a column")
    print("[3] Show top 10 entries")
    
    print("\n[99]Exit")
    
    print("\n")
    menu_choice = int(input("--> "))
    return menu_choice
        