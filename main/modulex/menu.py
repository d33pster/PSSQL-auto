#MENU

def menu():
    print("Menu \\-/ ")
    print("[0]Show active DataFrame")
    print("[1]Change active DataFrame")
    print("[2]Show")
    print("[3]drop a column")
    print("\n[99]Exit")
    
    print("\n")
    menu_choice = int(input("--> "))
    return menu_choice
        