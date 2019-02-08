import Pyro4

class Greeting:
    def __init__(self, ns):
        self.ns = ns

    @Pyro4.expose
    def greet(self, name):
        return 'Hello {0}'.format(name)

    @Pyro4.expose
    def get_status(self):
        return 'active'

if __name__ == '__main__':
    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS() # find the name server
    greeting = Greeting(ns)
    uri = daemon.register(greeting) # register as a Pyro object
    ns.register('server.greeting', uri, metadata={'server'}) # register object with a name in the name server

    print('ready')
    daemon.requestLoop()
