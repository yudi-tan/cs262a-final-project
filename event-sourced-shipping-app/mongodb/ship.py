import abc
import os
import json
from typing import List, Dict

import pymongo, ray

from util import Actor, Event

SNAPSHOTS_DIR = 'data/snapshots'


@ray.remote
class Ship(Actor):
    def __init__(self, name: str, location: str, db="mongodb://localhost:27017/", replay: bool=False):
        self.name: str
        self.location: str
        self.owner: str
        self.cargo: List[str]

        self.loc_offset = 0
        self.owner_offset = 0
        self.cargo_offset = 0

        self.client = pymongo.MongoClient(db)
        self.db = self.client.shipping_app
        self.collection = self.db.ship_logs

        if not replay:
            self.on(Ship.Creation(name, location))
            return

        path = os.path.join(SNAPSHOTS_DIR, name)
        if not os.path.exists(path):
            return
        snapshots = os.listdir(path)
        if snapshots:
            with open(os.path.join(path, max(snapshots)), 'r') as f:
                snapshot_dict = json.load(f)
            self.name = snapshot_dict["name"]
            self.location = snapshot_dict["location"]
            self.owner = snapshot_dict["owner"]
            self.cargo = snapshot_dict["cargo"]
            self.loc_offset = snapshot_dict["loc_offset"]
            self.owner_offset = snapshot_dict["owner_offset"]
            self.cargo_offset = snapshot_dict["cargo_offset"]

        self.eventHandler(self.collection.find({'ship': self.name}).next())

    # Create a snapshot of the current state
    def snapshot(self):
        snapshot_dict = {}
        snapshot_dict["name"] = self.name
        snapshot_dict["location"] = self.location
        snapshot_dict["owner"] = self.owner
        snapshot_dict["cargo"] = self.cargo
        snapshot_dict["loc_offset"] = self.loc_offset
        snapshot_dict["owner_offset"] = self.owner_offset
        snapshot_dict["cargo_offset"] = self.cargo_offset

        pathname = os.path.join(SNAPSHOTS_DIR, self.name, f'{self.loc_offset + self.owner_offset + self.cargo_offset}.json')
        os.makedirs(os.path.dirname(pathname), exist_ok=True)
        with open(pathname, 'w') as f:
            json.dump(snapshot_dict, f, indent = 4)

    def eventHandler(self, event: Event):
        if not isinstance(event, Event):
            return
        elif isinstance(event, Ship.Creation):
            self.name = event.ship
            self.location = event.port
            self.owner = ""
            self.cargo = list()
        elif isinstance(event, Ship.TransferOwnership):
            self.owner = event.owner
        elif isinstance(event, Ship.Departure):
            self.location = "SEA"
        elif isinstance(event, Ship.Arrival):
            self.location = event.port
        elif isinstance(event, Ship.Load):
            self.cargo.append(event.cargo)
        elif isinstance(event, Ship.Unload):
            self.cargo.remove(event.cargo)
        else:
            raise NotImplementedError

    def on(self, event: Event):
        if not isinstance(event, Event):
            return
        self.collection.insert_one({
            "type": event.__class__.__qualname__,
            **event.to_dict()
        })
        if isinstance(event, Ship.Creation):
            self.eventHandler(event)

    def depart(self, origin: str):
        self.getLocation()
        if self.location == "SEA" or self.location != origin:
            raise Ship.InvalidActionException
        self.on(Ship.Departure(self.name, origin))

    def arrive(self, target: str):
        self.getLocation()
        if self.location != "SEA":
            raise Ship.InvalidActionException
        self.on(Ship.Arrival(self.name, target))

    def load(self, cargo: str):
        self.getLocation()
        self.getCargo()
        if self.location == "SEA":
            raise Ship.InvalidActionException
        self.on(Ship.Load(self.name, cargo))

    def unload(self, cargo: str):
        self.getLocation()
        self.getCargo()
        if self.location == "SEA" or cargo not in self.cargo:
            raise Ship.InvalidActionException
        self.on(Ship.Unload(self.name, cargo))

    def __replay(self, cursor):
        count = 0
        for eventDict in cursor:
            event = Ship.eventFromDict(eventDict)
            self.eventHandler(event)
            count += 1
        return count

    def getName(self):
        return self.name

    def getLocation(self):
        self.loc_offset += self.__replay(self.collection.find({'ship': self.name, '$or': [{'type': 'Ship.Departure'}, {'type': 'Ship.Arrival'}]}).skip(self.loc_offset))
        return self.location

    def getOwner(self):
        self.owner_offset += self.__replay(self.collection.find({'ship': self.name, 'type': 'Ship.TransferOwnership'}).skip(self.owner_offset))
        return self.owner

    def getCargo(self):
        self.cargo_offset += self.__replay(self.collection.find({'ship': self.name, '$or': [{'type': 'Ship.Load'}, {'type': 'Ship.Unload'}]}).skip(self.cargo_offset))
        return self.cargo

    def getLog(self):
        return [entry for entry in self.collection.find({"ship": self.name})]

    class InvalidActionException(Exception):...

    class Creation(Event):
        def __init__(self, ship: str, port: str, happened: str=None):
            super().__init__(happened)
            self.ship = ship
            self.port = port

    class TransferOwnership(Event):
        def __init__(self, ship: str, owner: str, happened: str=None):
            super().__init__(happened)
            self.ship = ship
            self.owner = owner

    class Departure(Event):
        def __init__(self, ship: str, port: str, happened: str=None):
            super().__init__(happened)
            self.ship = ship
            self.port = port

    class Arrival(Event):
        def __init__(self, ship: str, port: str, happened: str=None):
            super().__init__(happened)
            self.ship = ship
            self.port = port

    class Load(Event):
        def __init__(self, ship: str, cargo: str, happened: str=None):
            super().__init__(happened)
            self.ship = ship
            self.cargo = cargo

    class Unload(Event):
        def __init__(self, ship: str, cargo: str, happened: str=None):
            super().__init__(happened)
            self.ship = ship
            self.cargo = cargo

    @staticmethod
    def eventFromDict(dict: Dict[str, str]):
        if dict["type"] == "Ship.Creation":
            return Ship.Creation(dict["ship"], dict["port"], dict["happened"])
        elif dict["type"] == "Ship.TransferOwnership":
            return Ship.TransferOwnership(dict["ship"], dict["owner"], dict["happened"])
        elif dict["type"] == "Ship.Departure":
            return Ship.Departure(dict["ship"], dict["port"], dict["happened"])
        elif dict["type"] == "Ship.Arrival":
            return Ship.Arrival(dict["ship"], dict["port"], dict["happened"])
        elif dict["type"] == "Ship.Load":
            return Ship.Load(dict["ship"], dict["cargo"], dict["happened"])
        elif dict["type"] == "Ship.Unload":
            return Ship.Unload(dict["ship"], dict["cargo"], dict["happened"])
        else:
            raise NotImplementedError
