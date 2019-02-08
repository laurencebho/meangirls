import Pyro4


class Frontend(object):
    def __init__(self, ns):
        self.ns = ns

    @Pyro4.expose
    def get_free(self):
        servers = ns.list(metadata_all={'server'})
        print(servers)
        for name in servers:
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

