class Fly_Table():
    def __init__ (self,name):
        self.name = name
        self.rows = {}

t = Fly_Table('test')
print(t.name)
row = [{"name":"tomex"},{"age":40}]
print(row)
t.rows[0] = row
print(t.rows[0])
