import Pyro4
from replica import Replica


class Frontend(object):
    def __init__(self, ns):
        self.ns = ns  
        self.main_replica = self.create_main_rep()
    
    def create_main_rep(self):
        replica = Replica(self.ns, 'main')
        return Pyro4.Proxy('PYRONAME:main')


    @Pyro4.expose
    def get_free(self):
        if self.main_replica.get_status() == 'active':
            return self.main_replica
        replicas = self.ns.list(metadata_all={'replica'})
        for name in replicas:
            proxy = Pyro4.Proxy('PYRONAME:' + name)
            status = proxy.get_status()
            if status == 'active':
                return proxy
        raise RuntimeError('no replicas are available right now')


if __name__ == '__main__':
    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS() # find the name server
    frontend = Frontend(ns)
    uri = daemon.register(frontend) # register as a Pyro object
    ns.register('frontend', uri) # register object with a name in the name server


    print('ready')
    daemon.requestLoop()

