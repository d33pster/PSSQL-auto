#MENU

def menu():
    print("Menu \\-/ ")
    #print("[0] ")
    print("[1] Change active DataFrame")
    print("[2] Show")
    print("[3] Drop a column")
    print("[4] Show top 10 entries")
    print("\n[99]Exit")
    
    print("\n")
    menu_choice = int(input("--> "))
    return menu_choice
        