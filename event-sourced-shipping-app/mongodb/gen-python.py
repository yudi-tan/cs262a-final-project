from typing import Callable, List
import random
import string

from util import *

# Program parameters
numPorts = 10
portNameLen = 3

numShips = 20
shipNameLen = 3

numCompanies = 0
companyNameLen = 5

cargoNameLen = 10

numEvents = 5000
snapshot_interval = 300
eventProbabilityOfCompany = 0
# number of events for companies = numEvents*eventProbabilityOfCompany

# Whether or not to test with the shipping company or just individual ships
testShippingCompany = False

outputFile = f"test e{numEvents} s{numShips} c{numCompanies} i{snapshot_interval} testShippingCompany={testShippingCompany}.py"

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


output = """import pymongo, shutil, time

from shipPython import *
from shipCompany import *

if os.path.exists('data/snapshots'):
    shutil.rmtree('data/snapshots/')

client = pymongo.MongoClient()
db = client.shipping_app
db.drop_collection("ship_logs")
db.drop_collection("company_logs")
t0 = time.time()
"""
output += f"\n# numEvents = {numEvents}, numShips = {numShips}, numCompanies = {numCompanies}\n"
insertion_ind = len(output)
sc = CompanyGen()
for name in allCompanyNames:
    sc.establish(name)


output += "allShips = []\n"

allShips = []
for name in allShipNames:
    startingPort = random.choice(allPortNames)
    ship = ShipGen(name, startingPort)
    allShips.append(ship)
    output += f"{name} = ShipPython('{name}', '{startingPort}')\n"
    if testShippingCompany:
        company = random.choice(allCompanyNames)
        sc.acquire(ship, company)
        output += f"sc_obj_ref = sc.acquire({name}, '{company}')\n"
    else:
        output += f"allShips.append({name})\n"
        output += f"{ship.name}.on(Ship.TransferOwnership('{ship.name}', ''))\n"

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
        sc.transfer(ship, old, new)
        if testShippingCompany:
            output += f"sc_obj_ref = sc.transfer({ship.name}, '{old}', '{new}')\n"
        else:
            output += f"{ship.name}.on(Ship.TransferOwnership('{ship.name}', '{new}'))\n"
    else:
        # Ship event
        ship = random.choice(allShips)
        name = ship.name
        action = random.choice(ship.getActions())
        if action == "getLocation":
            output += f"assert \"{ship.location}\" == {name}.getLocation()\n"
            Qs += 1
        elif action == "getOwner":
            # The get call ensures any potential transfer operations are completed first, as ships and the ship company run in parallel.
            # Any ship company operations that affect ships return the ObjectID of the ship's operation, hence the double get.
            output += f"assert \"{ship.owner}\" == {name}.getOwner()\n"
            Qs += 1
        elif action == "getCargo":
            output += f"for piece in {name}.getCargo():\n"
            output += f"\t assert piece in {ship.cargo}\n"
            Qs += 1
        elif action == "arrive":
            target = random.choice(allPortNames)
            output += f"{name}.arrive('{target}')\n"
            ship.arrive(target)
            Cs += 1
        elif action == "depart":
            origin = ship.location
            output += f"{name}.depart('{origin}')\n"
            ship.depart(origin)
            Cs += 1
        elif action == "load":
            cargo = "".join([random.choice(string.ascii_letters) for _ in range(cargoNameLen)])
            output += f"{name}.load('{cargo}')\n"
            ship.load(cargo)
            Cs += 1
        elif action == "unload":
            cargo = random.choice(ship.cargo)
            output += f"{name}.unload('{cargo}')\n"
            ship.unload(cargo)
            Cs += 1
    if count % snapshot_interval == 0:
            output += "for ship in allShips:\n"
            output += "\tship.snapshot()\n"
    count += 1
output = output[:insertion_ind] + f"# num_queries = {Qs}, num_commands = {Cs}\n\n" + output[insertion_ind:]
output += "\n"

for name in allShipNames:
    output += f"replayed = ShipPython('{name}', '', replay=True)\n"
    output += f"assert replayed.getName() == {name}.getName()\n"
    output += f"assert replayed.getLocation() == {name}.getLocation()\n"
    output += f"assert replayed.getOwner() == {name}.getOwner()\n"
    output += f"assert replayed.getCargo() == {name}.getCargo()\n"


output += "t1 = time.time()\n"
output += "print(t1 - t0)\n"
with open(outputFile, "w") as f:
    f.write(output)
