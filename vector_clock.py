import copy

class VectorClock:
    def __init__(self, id=None):
        self.vector = {}
        if id is not None:
            self.vector[id] = 0

    def merge(self, clock):
        vector = clock.get_vector()
        for id, val in vector.items():
            self.vector[id] = max(self.vector[id], val) if id in self.vector else val

    def inc(self, id):
        self.vector[id] += 1

    def get_vector(self):
        return self.vector

    def get_val(self, id):
        return self.vector[id] if id in self.vector else 0

    def set_val(self, id, val):
        self.vector[id] = val

    def is_more_recent_than(self, clock): 
        vector = clock.get_vector()
        for id, val in vector.items():
            if val not in self.vector:
                return False
            elif val > vector[id]:
                return False 
        return True

    def get_copy(self):
        return copy.deepcopy(self)


