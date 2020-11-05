import ray

from ship import *
from shipCompany import *

ray.init()
# I really don't know much about sailing so I will pretend that the sky is the sea...
ual: ShipCompany = ShipCompany.remote("UAL")
aa: ShipCompany = ShipCompany.remote("AA")

s1: Ship = Ship.remote("Titanic", "SFO")
s2: Ship = Ship.remote("Boeing", "OAK")
s3: Ship = Ship.remote("Airbus", "YVR")
s4: Ship = Ship.remote("Ivy", "SFO")

ual.acquire.remote(s1)
ual.acquire.remote(s2)
aa.acquire.remote(s3)
aa.acquire.remote(s4)

s1.depart.remote("SFO")
s2.depart.remote("OAK")
s1.arrive.remote("PVG")
s3.load.remote("passenger")
s3.depart.remote("YVR")
s2.arrive.remote("DXB")
ual.unacquire.remote(s1)
aa.acquire.remote(s1)
# how should we achieve atomicity here?
s4.load.remote("some cargo")
s3.arrive.remote("SFO")
ual.unacquire.remote(s2)
aa.acquire.remote(s2)
s4.depart.remote("SFO")
s4.arrive.remote("LAX")
s3.unload.remote("passenger")
aa.unacquire.remote(s3)
ual.acquire.remote(s3)
s4.unload.remote("some cargo")

ualLog = ray.get(ual.getGlobalLogStream.remote())
aaLog = ray.get(aa.getGlobalLogStream.remote())
