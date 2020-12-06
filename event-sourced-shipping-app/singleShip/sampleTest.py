import ray

from main import *

ray.init()
s1: Ship = Ship.remote("Titanic", "B")

s1.load.remote("some cargo")
s1.depart.remote("B")
s1.arrive.remote("I")
s1.unload.remote("some cargo")

log = ray.get(s1.getLog.remote())

# Complete replay test
s2 = Ship.replay(log)
assert log == ray.get(s2.getLog.remote())
assert ray.get(s1.getState.remote()) == ray.get(s2.getState.remote())

# Time travel test
s3 = Ship.replay(log[:3])
_, s3_location, s3_cargo = ray.get(s3.getState.remote())
assert log[:3] == ray.get(s3.getLog.remote())
assert s3_location == "SEA" and s3_cargo == ["some cargo"]
