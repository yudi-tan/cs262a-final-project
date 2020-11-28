import abc
import os
import json
from typing import List, Dict

import pymongo, ray

from util import Actor, Event

SNAPSHOTS_DIR = 'data/snapshots'

# class ref_src:
#     def __init__(self, parent_ref):
#         self.parent_ref: ray.actor.ActorHandle = parent_ref
#         self.parent_name = ray.get(parent_ref.get_name.remote())

@ray.remote
class Ship(Actor):
    def __init__(self, name: str, location: str, db="mongodb://localhost:27017/", replay: bool=False):
        self.name: str
        self.location: str
        # self.owner: ShipCompany
        self.owner: str
        self.cargo: List[str]
        # self.log: List[Event] = list()
        self.offset = 0
        self.client = pymongo.MongoClient(db)
        self.db = self.client.shipping_app
        self.collection = self.db.ship_logs
        if replay:
            path = os.path.join(SNAPSHOTS_DIR, name)
            if os.path.exists(path):
                snapshots = os.listdir(path)
                if snapshots:
                    with open(os.path.join(path, max(snapshots)), 'r') as f:
                        snapshot_dict = json.load(f)
                    self.name = snapshot_dict["name"]
                    self.location = snapshot_dict["location"]
                    self.owner = snapshot_dict["owner"]
                    self.cargo = snapshot_dict["cargo"]
                    self.offset = snapshot_dict["offset"]

            selected = self.collection.find({'ship': name}).skip(self.offset)
            for eventDict in selected:
                event = Ship.eventFromDict(eventDict)
                self.offset += 1
                self.eventHandler(event)
        else:
            self.on(Ship.Creation(name, location))

    # Create a snapshot of the current state
    def snapshot(self):
        snapshot_dict = {}
        snapshot_dict["name"] = self.name
        snapshot_dict["location"] = self.location
        snapshot_dict["owner"] = self.owner
        snapshot_dict["cargo"] = self.cargo
        snapshot_dict["offset"] = self.offset

        pathname = os.path.join(SNAPSHOTS_DIR, self.name, f'{self.offset}.json')
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
        self.collection.insert_one({
            "type": event.__class__.__qualname__,
            **event.to_dict()
        })
        # self.log.append(event)
        self.offset += 1
        self.eventHandler(event)

    def depart(self, origin: str):
        if self.location == "SEA" or self.location != origin:
            raise Ship.InvalidActionException
        self.on(Ship.Departure(self.name, origin))

    def arrive(self, target: str):
        if self.location != "SEA":
            raise Ship.InvalidActionException
        self.on(Ship.Arrival(self.name, target))

    def load(self, cargo: str):
        if self.location == "SEA":
            raise Ship.InvalidActionException
        self.on(Ship.Load(self.name, cargo))

    def unload(self, cargo: str):
        if self.location == "SEA" or cargo not in self.cargo:
            raise Ship.InvalidActionException
        self.on(Ship.Unload(self.name, cargo))

    def getName(self):
        return self.name

    def getLocation(self):
        return self.location

    def getOwner(self):
        return self.owner

    def getCargo(self):
        return self.cargo

    def getLog(self):
        return [entry for entry in self.collection.find({"ship": self.name})]
        # return self.log

    class InvalidActionException(Exception):...

    class Creation(Event):
        def __init__(self, ship: str, port: str, happened: str=None):
            super().__init__(happened)
            self.ship = ship
            self.port = port

    class TransferOwnership(Event):
        # def __init__(self, owner: ShipCompany):
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
