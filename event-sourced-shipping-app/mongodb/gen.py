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

outputFile = f"test e{numEvents} s{numShips} c{numCompanies} i{snapshot_interval}.py"

########
random.seed(0)

allPortNames = ["Port" + str(i) for i in range(numPorts)]
# allPortNames = []
# while len(allPortNames) < numPorts:
#     name = "".join([random.choice(string.ascii_uppercase) for _ in range(portNameLen)])
#     allPortNames.append(name) if name not in allPortNames else None

allShipNames = ["Ship" + str(i) for i in range(numShips)]
# allShipNames = []
# while len(allShipNames) < numShips:
#     name = "".join([random.choice(string.ascii_letters) for _ in range(shipNameLen)])
#     allShipNames.append(name) if name not in allShipNames else None

allCompanyNames = ["Company" + str(i) for i in range(numCompanies)]
# allCompanyNames = []
# while len(allCompanyNames) < numCompanies:
#     name = "".join([random.choice(string.ascii_letters) for _ in range(companyNameLen)])
#     allCompanyNames.append(name) if name not in allCompanyNames else None


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
        ret = ["getLocation", "getOwner"]

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
        # self.ships: Dict[str, List[Ship]] = {}
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
    allShips.append(ShipGen(name, startingPort))
    output += f"{name}: Ship = Ship.remote('{name}', '{startingPort}')\n"
    output += f"sc.acquire.remote({name}, '{random.choice(allCompanyNames)}')\n"

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
        output += f"sc.transfer.remote({ship}, '{old}', '{new}')\n"
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
