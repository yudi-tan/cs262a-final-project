import ray

from main import *

ray.init()
s1: Ship = Ship.remote("Titanic", "B")

s1.load.remote("some cargo")
s1.depart.remote("B")
s1.arrive.remote("I")
s1.unload.remote("some cargo")

log = s1.getLog.remote()
log = ray.get(log)

s2 = Ship.replay(log)
