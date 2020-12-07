from typing import Callable, List
import random
import string

from util import *

# Program parameters
numPorts = 10
portNameLen = 3

numShips = 20
shipNameLen = 3

numCompanies = 3
companyNameLen = 5

cargoNameLen = 10

numEvents = 1000
snapshot_interval = 100
eventProbabilityOfCompany = 0.05
# number of events for companies = numEvents*eventProbabilityOfCompany

# Owner validation requires waiting for ship company operations to complete before validating the ships, which could be bad for performance.
validateOwner = True

outputFile = f"test e{numEvents} s{numShips} c{numCompanies} i{snapshot_interval}.py"

########
random.seed(0)

allPortNames = ["Port" + str(i) for i in range(numPorts)]
allShipNames = ["Ship" + str(i) for i in range(numShips)]
allCompanyNames = ["Company" + str(i) for i in range(numCompanies)]


class ShipGen:
    def __init__(self, name: str, location: str):
        self.name: str = name
        self.location: str = location
        self.owner: str = ""
        self.cargo: List[str] = []

    def depart(self, origin: str):
        self.location = "SEA"

    def arrive(self, target: str):
        self.location = target

    def load(self, cargo: str):
        self.cargo.append(cargo)

    def unload(self, cargo: str):
        self.cargo.remove(cargo)

    def getActions(self) -> List[str]:
        ret = ["getLocation"]

        if validateOwner:
            ret += ["getOwner"]

        if self.cargo:
            ret += ["getCargo"]

        if self.location == "SEA":
            # we can only arrive
            ret += ["arrive"]
        else:
            ret += ["depart", "load"]
            if self.cargo:
                ret += ["unload"]
        return ret

class CompanyGen:
    def __init__(self):
        self.ships = {}

    def establish(self, name):
        self.ships[name] = list()

    def acquire(self, ship, company):
        self.ships[company].append(ship)
        ship.owner = company

    def unacquire(self, ship, company):
        self.ships[company].remove(ship)
        ship.owner = ""

    def transfer(self, ship, oldCompany, newCompany):
        self.ships[oldCompany].remove(ship)
        self.ships[newCompany].append(ship)
        ship.owner = newCompany


output = """import pymongo, ray, shutil

from ship import *
from shipCompany import *

if os.path.exists('data/snapshots'):
    shutil.rmtree('data/snapshots/')

ray.init()

client = pymongo.MongoClient()
db = client.shipping_app
db.drop_collection("ship_logs")
db.drop_collection("company_logs")

sc: ShipCompany = ShipCompany.remote()
sc_obj_ref: ray._raylet.ObjectID
"""
output += f"\n# numEvents = {numEvents}, numShips = {numShips}, numCompanies = {numCompanies}\n"
insertion_ind = len(output)
sc = CompanyGen()
for name in allCompanyNames:
    sc.establish(name)
    output += f"sc.establish.remote('{name}')\n"

allShips = []
for name in allShipNames:
    startingPort = random.choice(allPortNames)
    ship = ShipGen(name, startingPort)
    allShips.append(ship)
    output += f"{name}: Ship = Ship.remote('{name}', '{startingPort}')\n"
    company = random.choice(allCompanyNames)
    sc.acquire(ship, company)
    output += f"sc_obj_ref = sc.acquire.remote({name}, '{company}')\n"

output += "\n"

count = 0
Qs = 0
Cs = 0
while count < numEvents:
    if random.random() < eventProbabilityOfCompany:
        # Company event
        old, new = random.sample(allCompanyNames, 2)
        if len(sc.ships[old]) == 0:
            continue
        ship = random.choice(sc.ships[old])
        output += f"sc_obj_ref = sc.transfer.remote({ship.name}, '{old}', '{new}')\n"
        sc.transfer(ship, old, new)
    else:
        # Ship event
        ship = random.choice(allShips)
        name = ship.name
        action = random.choice(ship.getActions())
        if action == "getLocation":
            output += f"assert \"{ship.location}\" == ray.get({name}.getLocation.remote())\n"
            Qs += 1
        elif action == "getOwner":
            # The get call ensures any potential transfer operations are completed first, as ships and the ship company run in parallel.
            # Any ship company operations that affect ships return the ObjectID of the ship's operation, hence the double get.
            output += f"ray.get(ray.get(sc_obj_ref))\n"
            output += f"assert \"{ship.owner}\" == ray.get({name}.getOwner.remote())\n"
            Qs += 1
        elif action == "getCargo":
            output += f"for piece in ray.get({name}.getCargo.remote()):\n"
            output += f"\t assert piece in {ship.cargo}\n"
            Qs += 1
        elif action == "arrive":
            target = random.choice(allPortNames)
            output += f"{name}.arrive.remote('{target}')\n"
            ship.arrive(target)
            Cs += 1
        elif action == "depart":
            origin = ship.location
            output += f"{name}.depart.remote('{origin}')\n"
            ship.depart(origin)
            Cs += 1
        elif action == "load":
            cargo = "".join([random.choice(string.ascii_letters) for _ in range(cargoNameLen)])
            output += f"{name}.load.remote('{cargo}')\n"
            ship.load(cargo)
            Cs += 1
        elif action == "unload":
            cargo = random.choice(ship.cargo)
            output += f"{name}.unload.remote('{cargo}')\n"
            ship.unload(cargo)
            Cs += 1
    if count % snapshot_interval == 0:
        output += "for company in ray.get(sc.getState.remote()).values():\n"
        output += "\tfor ship in company:\n"
        output += "\t\tship.snapshot.remote()\n"
    count += 1
output = output[:insertion_ind] + f"# num_queries = {Qs}, num_commands = {Cs}\n\n" + output[insertion_ind:]
output += "\n"

for name in allShipNames:
    output += f"replayed = Ship.remote('{name}', '', replay=True)\n"
    output += f"assert ray.get(replayed.getName.remote()) == ray.get({name}.getName.remote())\n"
    output += f"assert ray.get(replayed.getLocation.remote()) == ray.get({name}.getLocation.remote())\n"
    output += f"assert ray.get(replayed.getOwner.remote()) == ray.get({name}.getOwner.remote())\n"
    output += f"assert ray.get(replayed.getCargo.remote()) == ray.get({name}.getCargo.remote())\n"

output += "\nlog = ray.get(sc.getGlobalLogStream.remote())\n"

with open(outputFile, "w") as f:
    f.write(output)
