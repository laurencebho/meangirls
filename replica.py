import Pyro4, uuid, random, csv, threading, time
from vector_clock import VectorClock

class Replica:
    def __init__(self, ns, id=None):
        self.ns = ns
        self.id = id if id is not None else str(uuid.uuid4())

        daemon = Pyro4.Daemon()
        uri = daemon.register(self) 

        loop_thread = threading.Thread(target=daemon.requestLoop)
        print('Replica {id} ready'.format(id=self.id))
        loop_thread.start()

        self.movies = self.read_movies()
        print('movies initialised')

        ns.register(self.id, uri, metadata={'replica'}) 

        self.update_log = []
        self.applied_updates = {}
        self.replica_ts = VectorClock(id)
        self.ts = VectorClock(id)
        print('clock set')

        gossip_thread = threading.Thread(target=self.gossip)
        gossip_thread.start()

        self.status = 'active'
        status_thread = threading.Thread(target=self.sim_status)
        status_thread.start()


    @Pyro4.expose
    def get_id(self):
        return self.id


    @Pyro4.expose
    def get_movies(self):
        return self.movies


    #read initial movie data from file
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


    def apply_update(self, u):
        if u['update_id'] not in self.applied_updates:
            if u['uid'] not in self.movies:
                self.movies[u['uid']] = {}
            self.movies[u['uid']][u['movie_id']] = u['rating']
            self.applied_updates.add(u['update_id'])


    def handle_update(self, u):
        self.replica_ts.inc(self.id) #inc rep timestamp
        TS = u['prev'].get_copy()
        TS.set_val(self.id, self.replica_ts.get_val(id))
        self.update_log.append((u, TS))

        if self.ts.is_more_recent_than(u['prev']):
            self.apply_update(u)
            self.ts.merge(TS)
        return TS

    
    def handle_query(self, q):
        while True:
            if self.ts.is_more_recent_than(q['prev']):
                return find_rating(q['uid'], q['movie_id']), self.ts
            else:
                time.sleep(0.5)


    def find_rating(self, uid, movie_id):
        if uid in self.movies:
            if movie_id in self.movies[uid]:
                return str(self.movies[uid][movie_id])
        return None


    def sort_and_apply_updates(self):
        l = len(self.update_log) - 1
        for i in range(l):
            for j in range(l - i):
                if self.update_log[j][0]['prev'].is_more_recent_than(self.update_log[j+1][0]['prev']):
                    self.update_log[j], self.update_log[j+1] = self.update_log[j+1], self.update_log[j]
        for entry in self.update_log:
            self.apply_update(entry[0])



    @Pyro4.expose
    def get_gossip_data(self):
        return self.update_log, self.replica_ts


    def gossip(self):
        while True:
            time.sleep(5)
            replicas = self.ns.list(metadata_all={'replica'})
            del replicas[self.id] # remove itself from the dict
            all_updates = {}
            for name in replicas:
                with Pyro4.Proxy('PYRONAME:' + name) as proxy:
                    try:
                        status = proxy.get_status()
                        if status == 'active':
                            log, rep_ts = proxy.get_gossip_data()
                            all_updates.union(log)
                            self.replica_ts.merge(rep_ts)
                    except: #if the replica goes offline unexpectedly
                        pass
            all_updates = [u for u in all_updates if u not in self.update_log]
            print(all_updates)
            self.update_log.extend(all_updates)
            self.sort_and_apply_updates() #sort updates, and apply them again in the sorted order


    def sim_status(self):
        while True:
            time.sleep(5)
            r = random.random()
            if self.status == 'active':
                if r < 0.05:
                    self.status = 'offline'
                    raise RuntimeError('replica has gone offline')
                elif r < 0.1:
                    self.status = 'overloaded' #does not immediately crash, but cannot take on more work
            else: #if offline or overloaded, probability of becoming active again is 0.8
                if r < 0.8:
                    self.status = 'active'

            
    @Pyro4.expose
    def get_status(self):
        return self.status 


if __name__ == '__main__':
    ns = Pyro4.locateNS() # find the name server
    replica = Replica(ns)
