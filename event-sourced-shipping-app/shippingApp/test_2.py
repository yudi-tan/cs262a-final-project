import ray

from ship import *
from shipCompany_2 import *

ray.init()
# I really don't know much about sailing so I will pretend that the sky is the sea...
sc: ShipCompany = ShipCompany.remote()
sc.establish.remote("UAL")
sc.establish.remote("AA")

s1: Ship = Ship.remote("Titanic", "SFO")
s2: Ship = Ship.remote("Boeing", "OAK")
s3: Ship = Ship.remote("Airbus", "YVR")
s4: Ship = Ship.remote("Ivy", "SFO")

sc.acquire.remote(s1, "UAL")
sc.acquire.remote(s2, "UAL")
sc.acquire.remote(s3, "AA")
sc.acquire.remote(s4, "AA")

s1.depart.remote("SFO")
s2.depart.remote("OAK")
s1.arrive.remote("PVG")
s3.load.remote("passenger")
s3.depart.remote("YVR")
s2.arrive.remote("DXB")
sc.transfer.remote(s1, "UAL", "AA")
# how should we achieve atomicity here?
s4.load.remote("some cargo")
s3.arrive.remote("SFO")
sc.transfer.remote(s2, "UAL", "AA")
s4.depart.remote("SFO")
s4.arrive.remote("LAX")
s3.unload.remote("passenger")
sc.transfer.remote(s3, "AA", "UAL")
s4.unload.remote("some cargo")

log = ray.get(sc.getGlobalLogStream.remote())
