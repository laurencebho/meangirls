import Pyro4

def get_id():
    while True:
        uid = input('Enter user ID:')
        break
        try:
            uid = int(uid)
        except:
            print('Invalid ID - must be an integer')
    return uid

def get_choice():
    while True:
        choice = input('Enter choice (w)rite, (r)ead, (q)uit')
        if choice in ['w', 'r', 'q']:
            return choice
        print('Not a valid choice.')

frontend = Pyro4.Proxy('PYRONAME:frontend')
replica = frontend.get_free()
