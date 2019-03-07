import copy

class VectorClock:

    def __init__(self, vector=None):
        self.vector = vector if vector is not None else {}

    def merge(self, vector):
        for id, val in vector.items():
            self.vector[id] = max(self.vector[id], val) if id in self.vector else val

    def inc(self, id):
        if id in self.vector:
            self.vector[id] += 1
        else:
            self.vector[id] = 1

    def get_vector(self):
        return self.vector

    def get_val(self, id):
        return self.vector[id] if id in self.vector else 0

    def set_val(self, id, val):
        self.vector[id] = val

    def is_more_recent_than(self, vector): 
        for id, val in vector.items():
            if id not in self.vector:
                return False
            elif val > self.vector[id]:
                return False 
        return True
