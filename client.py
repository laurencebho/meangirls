import Pyro4

name = input('name:')

frontend = Pyro4.Proxy('PYRONAME:frontend')
greeter = frontend.get_free()
greeting = greeter.greet(name)
print(greeting)
