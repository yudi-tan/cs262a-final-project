import abc
import os
import json
from typing import List

import ray

from util import Actor, Event

SNAPSHOTS_DIR = 'data/snapshots'

# class ref_src:
#     def __init__(self, parent_ref):
#         self.parent_ref: ray.actor.ActorHandle = parent_ref
#         self.parent_name = ray.get(parent_ref.get_name.remote())

@ray.remote
class Ship(Actor):
    def __init__(self, name: str, location: str, replay: bool = False):
        self.name: str
        self.location: str
        # self.owner: ShipCompany
        self.owner: str
        self.cargo: List[str]
        self.log: List[Event] = list()
        self.offset = 0
        if not replay:
            self.on(Ship.Creation(name, location))

    # Create a snapshot of the current state
    def snapshot(self):
        snapshot_dict = {}
        snapshot_dict["location"] = self.location
        snapshot_dict["owner"] = self.offset
        snapshot_dict["cargo"] = self.cargo
        snapshot_dict["log"] = self.log
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
        self.log.append(event)
        self.offset += 1
        assert len(self.log) == self.offset
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
        # replay(getName()).location

    def getLocation(self):
        return self.location

    def getOwner(self):
        return self.owner

    def getCargo(self):
        return self.cargo

    def getLog(self):
        return self.log

    @staticmethod
    def replay(events: List[Event]):
        assert len(events) > 0 and isinstance(events[0], Ship.Creation)
        ret = Ship.remote(events[0].ship, events[0].port, True)

        path = os.path.join(SNAPSHOTS_DIR, ret.name)
        if os.path.exists(path):
            snapshots = os.listdir(path)
            if snapshots:
                with open(os.path.join(SNAPSHOTS_DIR, max(snapshots)), 'r') as f:
                    snapshot_dict = json.load(f)
                ret.location = snapshot_dict["location"]
                ret.offset = snapshot_dict["owner"]
                ret.cargo = snapshot_dict["cargo"]
                ret.log = snapshot_dict["log"]
                ret.offset = snapshot_dict["offset"]

        while ret.offset < len(events):
            self.eventHandler(event)
            ret.offset += 1
        ret.events = events

        return ret

    class InvalidActionException(Exception):...

    class Creation(Event):
        def __init__(self, ship: str, port: str):
            super().__init__()
            self.ship = ship
            self.port = port

    class TransferOwnership(Event):
        # def __init__(self, owner: ShipCompany):
        def __init__(self, owner: str):
            super().__init__()
            self.owner = owner

    class Departure(Event):
        def __init__(self, ship: str, port: str):
            super().__init__()
            self.ship = ship
            self.port = port

    class Arrival(Event):
        def __init__(self, ship: str, port: str):
            super().__init__()
            self.ship = ship
            self.port = port

    class Load(Event):
        def __init__(self, ship: str, cargo: str):
            super().__init__()
            self.ship = ship
            self.cargo = cargo

    class Unload(Event):
        def __init__(self, ship: str, cargo: str):
            super().__init__()
            self.ship = ship
            self.cargo = cargo
