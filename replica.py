import Pyro4, uuid, random, csv, threading, time

class Replica:
    def __init__(self, ns, id=None):
        self.ns = ns
        self.id = id if id is not None else str(uuid.uuid4())

        daemon = Pyro4.Daemon()
        uri = daemon.register(self) 

        loop_thread = threading.Thread(target=daemon.requestLoop)
        print('Replica {id} ready'.format(id=self.id))
        loop_thread.start()


        self.movies = self.init_movies()
        print('movies initialised')

        ns.register(self.id, uri, metadata={'replica'}) 

        self.updates = []
        self.clock = self.init_clock()
        print('clock set')
        gossip_thread = threading.Thread(target=self.gossip)
        gossip_thread.start()


    @Pyro4.expose
    def get_id(self):
        return self.id
    
    @Pyro4.expose
    def get_updates(self, old_timestamp):
        timestamp_diff = self.clock[self.id] - old_timestamp
        if timestamp_diff > 0:
            return {self.id : self.updates[-timestamp_diff:]}
        return {self.id: []}

    @Pyro4.expose
    def get_timestamp(self):
        return self.clock[self.id]

    @Pyro4.expose
    def get_movies(self):
        return self.movies


    def read_movies(self):
        movies = {}
        with open('./ratings/ratings.csv', newline='') as f:
            reader = csv.reader(f)
            reader.__next__() #skip first line
            for row in reader:
                uid = row[0]
                if uid not in movies:
                    movies[uid] = {}
                movies[uid][row[1]] = row[2]
        return movies


    def init_movies(self):
        replicas = self.ns.list(metadata_all={'replica'})
        if len(replicas) == 0:
            return self.read_movies()
        n = 3
        while True:
            n -= 1
            for name in replicas:
                proxy = Pyro4.Proxy('PYRONAME:' + name)
                status = proxy.get_status()
                if status == 'active':
                    return proxy.get_movies()
            if n == 0:
                raise RuntimeError('couldn\'t initialise replica - all others unavailable')
            time.sleep(0.1)


    def init_clock(self): #doesn't add self to clock currently
        replicas = self.ns.list(metadata_all={'replica'})
        clock = {}
        for name in replicas:
            clock[name] = 0
        print(clock)
        return clock


    def inc_clock(self):
        self.clock[self.id] += 1;


    def update_clock(self, vector):
        for id, val in vector.items():
            self.clock[id] = val

    @Pyro4.expose
    def update_rating(self, uid, movie_id, rating, add_to_updates=True):
        if uid not in self.movies:
            self.movies[uid] = {}
        self.movies[uid][movie_id] = rating
        if add_to_updates:
            self.updates.append((uid, movie_id, rating))
        print('movie {0} for user {1} rated {2}'.format(movie_id, uid, rating))
        self.inc_clock()
        return (self.id, self.clock[self.id])


    def find_rating(self, uid, movie_id):
        if uid in self.movies:
            if movie_id in self.movies[uid]:
                return str(self.movies[uid][movie_id])
        return 'Not found'

    @Pyro4.expose
    def get_rating(self, uid, movie_id, timestamp=None):

        if timestamp is not None:
            # if replica is up to date with client then give rating back, else wait
            if self.clock[timestamp[0]] >= timestamp[1]:
                return self.find_rating(uid, movie_id)
            else:
                print('waiting for updates')
                time.sleep(1)
                return self.get_rating(uid, movie_id, timestamp)
        return self.find_rating(uid, movie_id)


    # returns True if a has priority over b
    # creates a global order which can be used to handle concurrent writes
    def has_priority(self, a, b):
        if hash(a) > hash(b):
            return True
        return False


    def resolve_clash(self, my_timestamp, their_timestamp, rating):
        if my_timestamp > their_timestamp:
            self.update_rating(*rating, False)
        elif my_timestamp == their_timestamp:
            # if I have priority, apply their update after mine
            if self.has_priority(self.id, name):
                self.update_rating(*rating, False)
            else:
                self.inc_clock()
        else:
            # inc clock even if update doesn't need to be applied
            self.inc_clock() 


    def apply_updates(self, updates):
        update_count = len(self.updates)
        for name, ratings in updates.items():
            neighbour_timestamp = self.clock[name] # and if old value + neighbour_update_count != this then handle
            neighbour_update_count = len(ratings)
            for i, rating in enumerate(ratings):
                clash = False
                for j, update in enumerate(reversed(self.updates)):
                    #if uid and movie id match there is a clash
                    if update[0] == rating[0] and update[1] == rating[1]:
                        my_timestamp = self.clock[self.id] - j
                        their_timestamp = neighbour_timestamp - (neighbour_update_count - i)
                        self.resolve_clash(my_timestamp, their_timestamp, rating)
                        clash = True
                        break
                if not clash:
                    print('rating is {0}'.format(rating))
                    self.update_rating(*rating, False)


    def gossip(self):
        while True:
            time.sleep(5)
            replicas = self.ns.list(metadata_all={'replica'})
            del replicas[self.id] # remove itself from the dict
            updates = {}
            vector = {}
            for name in replicas:
                proxy = Pyro4.Proxy('PYRONAME:' + name)
                status = proxy.get_status()
                if status == 'active':
                    rep_id = proxy.get_id()
                    old_timestamp = self.clock[rep_id] if rep_id in self.clock else 0 
                    print('old timestamp: {0}'.format(old_timestamp))
                    updates.update(proxy.get_updates(old_timestamp))
                    vector[name] = proxy.get_timestamp()
            self.update_clock(vector)
            print(updates)
            self.apply_updates(updates)


    @Pyro4.expose
    def greet(self, name):
        return 'Hello {0}'.format(name)

    @Pyro4.expose
    def get_status(self):
        r = random.random()
        if r < 0.01:
            return 'offline'
        elif r < 0.03:
            return 'overloaded'
        return 'active'


if __name__ == '__main__':
    ns = Pyro4.locateNS() # find the name server
    replica = Replica(ns)

