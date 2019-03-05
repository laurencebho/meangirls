import Pyro4

def get_id():
    while True:
        uid = input('Enter user ID: ')
        try:
            i = int(uid)
            break
        except ValueError:
            print('Invalid ID - must be an integer')
    return uid

def get_choice():
    valid_choices = ['w', 'W', 'r', 'R', 'q', 'Q', 'h', 'H']
    while True:
        choice = input('Enter choice (w)rite, (r)ead, (q)uit: ')
        if choice in :
            return choice
        print('Not a valid choice.')

def get_movie_id():
    while True:
        movie_id = input('Enter movie ID:')
        try:
            i = int(movie_id)
            break
        except ValueError:
            print('Invalid ID - must be an integer')
    return movie_id

def get_rating():
    while True:
        r = input('Enter rating: ')
        try:
            f = float(r)
            break
        except ValueError:
            print('Invalid rating - must be an integer or float')
    return r

def read(frontend, uid, movie_id, client_id);
    print('reading rating...')
    res = frontend.read(uid, movie_id, client_id)
    if res is not None:
        print('Rating for user {0} and movie {1}: {2}'.format(uid, movie_id, res))
    else:
        print('A rating for user {0} and movie {1} was not found.'.format(uid, movie_id))


def write(frontend, uid, movie_id, rating):
    print('writing rating...')
    frontend.write(uid, movie_id, rating, client_id)
    print('rating successfully written')


def show_help():
    print('')
    print('This system allows you to read and write movie ratings')
    print('Enter "w" to write (add or update) a rating, "r" to read a movie rating, and "q" to quit')
    print('')


def main():
    frontend = Pyro4.Proxy('PYRONAME:frontend')
    client_id = fronted.register_client()
    while True:
        choice = get_choice()
        if choice in ['q', 'Q']:
            break
        elif choice in ['r', 'R']:
            movie_id = get_movie_id()
            read(replica, uid, movie_id, timestamp)
        elif choice in ['w', 'W']:
            movie_id = get_movie_id()
            rating = get_rating()
            timestamp = write(replica, uid, movie_id, rating)
        elif choice in ['h', 'H']:
            show_help()


if __name__ == '__main__':
    main()
