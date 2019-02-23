import Pyro4, uuid, random, csv, threading

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


    def get_id(self):
        return self.id
    
    @Pyro4.expose
    def get_updates(self):
        return {self.id : self.updates}

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
        replicas = ns.list(metadata_all={'replica'})
        if len(replicas) == 0:
            return self.read_movies()
        for name in replicas:
            proxy = Pyro4.Proxy('PYRONAME:' + name)
            status = proxy.get_status()
            if status == 'active':
                return proxy.get_movies()
        raise RuntimeError('couldn\'t initialise replica - all others unavailable')


    def init_clock(self): #doesn't add self to clock currently
        replicas = ns.list(metadata_all={'replica'})
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


    def update_rating(self, uid, movie_id, rating):
        if uid not in self.movies:
            self.movies[uid] = {}
        self.movies[uid][movie_id] = rating
        self.updates += (uid, movie_id, rating)
        print('movie {0} for user {1} rated {2}'.format(movie_id, uid, rating))
        self.inc_clock()
        return (self.id, self.clock[self.id])


    def get_rating(self, uid, movie_id, timestamp=None):
        if timestamp is not None:
            # if replica is up to date with client then give rating back, else wait
            if self.clock[timestamp[0]] >= timestamp[1]:
                return find_rating(self, uid, movie_id)
            else:
                print('waiting for updates')
                time.sleep(1)
                return get_rating(self, uid, movie_id, timestamp)
        return find_rating(self, uid, movie_id)

        def find_rating(self, uid, movie_id):
            if uid in self.movies:
                if movie_id in self.movies[uid]:
                    return str(self.movies[uid][movie_id])
            return 'Not found'

    # returns True if a has priority over b
    # creates a global order which can be used to handle concurrent writes
    def has_priority(self, a, b):
        if hash(a) > hash(b):
            return True
        return False

    def apply_updates(self, updates): #obviously not consistent
        for name, ratings in updates.items():
            timestamp = self.clock[name]
            for i, r in enumerate(ratings):
                for j, update in enumerate(reversed(self.updates)):
                    if update[0] == r[0] and update[1] == r[1]:
                        my_timestamp = self.clock[self.id] - j
                        their_timestamp = timestamp + i
                        if my_timestamp > their_timestamp:
                            self.update_rating(*r)
                        elif my_timestamp == their_timestamp:
                            # if I have priority, apply their update after mine
                            if self.has_priority(self.id, name):
                                self.update_rating(*r)
                            else:
                                self.inc_clock()
                        else:
                            # inc clock even if update doesn't need to be applied
                            self.inc_clock() 
                        break


    def gossip(self):
        replicas = ns.list(metadata_all={'replica'})
        del replicas[self.id] # remove itself from the dict
        updates = {}
        vector = {}
        for name in replicas:
            proxy = Pyro4.Proxy('PYRONAME:' + name)
            status = proxy.get_status()
            if status == 'active':
                updates.update(proxy.get_updates())
                vector[name] = proxy.get_timestamp()
        self.update_clock(vector)
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

