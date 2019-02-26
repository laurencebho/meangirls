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
    while True:
        choice = input('Enter choice (w)rite, (r)ead, (q)uit: ')
        if choice in ['w', 'r', 'q']:
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

def read(replica, uid, movie_id, timestamp=None):
    print('reading rating...')
    rating = replica.get_rating(uid, movie_id, timestamp)
    print('Rating for user {0} and movie {1}: {2}'.format(uid, movie_id, rating))


def write(replica, uid, movie_id, rating):
    print('writing rating...')
    timestamp = replica.update_rating(uid, movie_id, rating)
    print('written rating')
    return timestamp


def main():
    frontend = Pyro4.Proxy('PYRONAME:frontend')
    replica = frontend.get_free()
    uid = get_id()
    timestamp = None
    while True:
        choice = get_choice()
        if choice == 'q':
            break
        elif choice == 'r':
            movie_id = get_movie_id()
            read(replica, uid, movie_id, timestamp)
        else:
            movie_id = get_movie_id()
            rating = get_rating()
            timestamp = write(replica, uid, movie_id, rating)


if __name__ == '__main__':
    main()
