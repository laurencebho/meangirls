import Pyro4, uuid, random, csv, threading, time, copy
from vector_clock import VectorClock

class Replica:
    #setup
    def __init__(self, ns, id=None):
        self.ns = ns
        self.id = id if id is not None else str(uuid.uuid4())

        daemon = Pyro4.Daemon()
        uri = daemon.register(self) 

        loop_thread = threading.Thread(target=daemon.requestLoop)
        print('Replica {id} ready'.format(id=self.id))
        loop_thread.start()

        self.movies = self.read_movies()

        ns.register(self.id, uri, metadata={'replica'}) 

        self.update_log = []
        self.applied_updates = set()
        self.replica_ts = VectorClock()
        self.ts = VectorClock()

        gossip_thread = threading.Thread(target=self.gossip)
        gossip_thread.start()

        self.status = 'active'
        status_thread = threading.Thread(target=self.sim_status)
        status_thread.start()


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


    #write an update to the movies dictionary
    def apply_update(self, u):
        if u['update_id'] not in self.applied_updates:
            if u['uid'] not in self.movies:
                self.movies[u['uid']] = {}
            self.movies[u['uid']][u['movie_id']] = u['rating']
            self.applied_updates.add(u['update_id'])


    #handle an update (i.e. a write) sent from FE to this replica
    @Pyro4.expose
    def handle_update(self, u):
        self.check_status()
        self.replica_ts.inc(self.id) #increment replica timestamp
        TS = copy.deepcopy(u['prev'])
        TS[self.id] = self.replica_ts.get_val(self.id)
        if 'update_id' not in u:
            u['update_id'] = uuid.uuid4()
        self.update_log.append((u, TS))

        if self.ts.is_more_recent_than(u['prev']):
            self.apply_update(u)
            self.ts.merge(TS)
        return TS

    #handle a query (i.e. a read) sent from FE to this replica
    @Pyro4.expose
    def handle_query(self, q):
        self.check_status()
        while True:
            if self.ts.is_more_recent_than(q['prev']):
                return self.find_rating(q['uid'], q['movie_id']), self.ts.get_vector()
            else:
                time.sleep(0.5)

    
    #read a rating from movies dictionary
    def find_rating(self, uid, movie_id):
        if uid in self.movies:
            if movie_id in self.movies[uid]:
                return str(self.movies[uid][movie_id])
        return None


    #periodically sort updates to ensure causal ordering
    def sort_and_apply_updates(self):
        l = len(self.update_log) - 1
        for i in range(l):
            for j in range(l - i):
                if VectorClock(vector=self.update_log[j][0]['prev']).is_more_recent_than(self.update_log[j+1][0]['prev']):
                    self.update_log[j], self.update_log[j+1] = self.update_log[j+1], self.update_log[j]
        for entry in self.update_log:
            self.apply_update(entry[0])
            self.ts.merge(entry[1])


    #called by other replicas - sends back update log and replica timestamp
    @Pyro4.expose
    def get_gossip_data(self):
        return self.update_log, self.replica_ts.get_vector()


    #gossip by pulling in updated - occurs every 5 secs
    def gossip(self):
        while True:
            time.sleep(5)
            if self.status != 'offline':
                replicas = self.ns.list(metadata_all={'replica'})
                del replicas[self.id] # remove itself from the dictionary of replicas
                all_updates = []
                for name in replicas:
                    with Pyro4.Proxy('PYRONAME:' + name) as proxy:
                        status = proxy.get_status()
                        if status != 'offline':
                            log, rep_ts = proxy.get_gossip_data()
                            all_updates.extend([u for u in log if u not in all_updates])
                            self.replica_ts.merge(rep_ts)
                all_updates = [u for u in all_updates if u not in self.update_log]
                self.update_log.extend(all_updates)
                self.sort_and_apply_updates() #sort updates, and apply them again in the sorted order


    #simulating replicas going offline or becoming overloaded
    def sim_status(self):
        while True:
            time.sleep(5)
            r = random.random()
            if self.status == 'active':
                if r < 0.15: #probablility of going offline = 0.15
                    self.status = 'offline'
                elif r < 0.3:
                    self.status = 'overloaded' #prob of becoming overloaded = 0.15

            else: #if offline or overloaded, probability of becoming active again is 0.8
                if r < 0.8:
                    self.status = 'active'


    #throws an error if replica is offline, to be caught by the frontend
    def check_status(self):
        if self.status == 'offline':
            raise RuntimeError('replica has gone offline')

            
    @Pyro4.expose
    def get_status(self):
        return self.status 


if __name__ == '__main__':
    ns = Pyro4.locateNS()
    replica = Replica(ns)
