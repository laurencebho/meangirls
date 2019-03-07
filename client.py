import Pyro4

def get_id():
    while True:
        uid = input('Enter user ID: ')
        print('')
        try:
            i = int(uid)
            print('User ID set to {0}'.format(uid))
            print('')
            break
        except ValueError:
            print('Invalid user ID - must be an integer')
            print('')
    return uid

def get_choice():
    valid_choices = ['w', 'W', 'r', 'R', 'q', 'Q', 'h', 'H', 'u', 'U']
    while True:
        print('Main Menu')
        print('=========')
        choice = input('Enter choice: ')
        print('')
        if choice in valid_choices:
            return choice
        print('Not a valid choice.')
        print('')

def get_movie_id():
    while True:
        movie_id = input('Enter movie ID: ')
        print('')
        try:
            i = int(movie_id)
            break
        except ValueError:
            print('Invalid movie ID - must be an integer')
            print('')
    return movie_id

def get_rating():
    while True:
        r = input('Enter rating: ')
        print('')
        try:
            f = float(r)
            break
        except ValueError:
            print('Invalid rating - must be an integer or float')
            print('')
    return r

def read(frontend, uid, movie_id, client_id):
    res = frontend.read(uid, movie_id, client_id)
    if res is not None:
        print('Rating for user {0} and movie {1}: {2}'.format(uid, movie_id, res))
    else:
        print('A rating for user {0} and movie {1} was not found.'.format(uid, movie_id))
    print('')


def write(frontend, uid, movie_id, rating, client_id):
    frontend.write(uid, movie_id, rating, client_id)
    print('Success: movie {0} rated {1}'.format(movie_id, rating))
    print('')


def show_welcome():
    print('Welcome to the movie rating system')
    while True:
        choice = input('Press h to view help, or any other key to continue: ')
        print('')
        if choice in ['h', 'H']:
            show_help()
        else: 
            break


def show_help():
    print('Help')
    print('====')
    print('First enter your user ID (e.g. 1)')
    print('Then from the main menu, enter your choice: ')
    print('    - "w" to write (add or update) a rating')
    print('    - "r" to read a movie rating')
    print('    - "q" to quit')
    print('    - "u" to change your user ID')
    print('')
    print('Any float is considered a valid rating')
    print('')


def main():
    frontend = Pyro4.Proxy('PYRONAME:frontend')
    client_id = frontend.register_client()
    show_welcome()
    uid = get_id()
    while True:
        choice = get_choice()
        if choice in ['q', 'Q']:
            print('Bye!')
            print('')
            break
        elif choice in ['r', 'R']:
            movie_id = get_movie_id()
            read(frontend, uid, movie_id, client_id)
        elif choice in ['w', 'W']:
            movie_id = get_movie_id()
            rating = get_rating()
            write(frontend, uid, movie_id, rating, client_id)
        elif choice in ['h', 'H']:
            show_help()
        elif choice in ['u', 'U']:
            uid = get_id()


if __name__ == '__main__':
    main()
