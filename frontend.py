import Pyro4, uuid
from replica import Replica
from contextlib import contextmanager
from vector_clock import VectorClock


class Frontend(object):
    def __init__(self, ns):
        self.ns = ns  
        self.main_replica = self.create_main_rep()
        self.clients = {}
    

    def create_main_rep(self):
        replica = Replica(self.ns, 'main')
        return Pyro4.Proxy('PYRONAME:main')


    #adding a client
    @Pyro4.expose
    def register_client(self):
        client_id = str(uuid.uuid4())
        self.clients[client_id] = VectorClock()
        return client_id


    #reading a rating
    @Pyro4.expose
    def read(self, uid, movie_id, client_id):
        q = {'uid': uid, 'movie_id': movie_id, 'prev': self.clients[client_id]}
        with handle_error():
            replica = self.get_free()
            self.read_ops.append(q)
            res, ts = replica.handle_query(q)
            del self.read_ops[self.read_ops.index(q)]
            self.clients[client_id].merge(ts)
            return res


    #writing a rating
    @Pyro4.expose
    def write(self, uid, movie_id, rating, client_id):
        u = {'uid': uid, 'movie_id': movie_id, 'rating': rating, 'prev': self.clients[client_id]}
        with handle_error():
            replica = self.get_free()
            self.read_ops.append(u)
            ts = replica.handle_update(u)
            del self.write_ops[self.write_ops.index(u)]
            self.clients[client_id].merge(ts)


    #handling the exception thrown when a replica goes offline
    @contextmanager
    def handle_error():
        try:
            yield
        except RuntimeError as err:
            print("Error: {0}".format(err))
            for q in self.read_ops:
                read(*q)
            for u in self.write_ops:
                write(*q)


    #tries to get an active replica a maximum of 3 times
    #checks the main replica first
    def get_free(self):
        if self.main_replica.get_status() == 'active':
            return self.main_replica
        replicas = self.ns.list(metadata_all={'replica'})
        n = 3
        while True:
            n -= 1
            for name in replicas:
                proxy = Pyro4.Proxy('PYRONAME:' + name)
                status = proxy.get_status()
                if status == 'active':
                    return proxy
            if n == 0:
                raise RuntimeError('All replicas are unavailable right now')
            time.sleep(0.5)


if __name__ == '__main__':
    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS() # find the name server
    frontend = Frontend(ns)
    uri = daemon.register(frontend) # register as a Pyro object
    ns.register('frontend', uri) # register object with a name in the name server

    print('frontend ready')
    daemon.requestLoop()

