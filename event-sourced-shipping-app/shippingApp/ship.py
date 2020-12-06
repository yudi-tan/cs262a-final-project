import abc
from typing import List

import ray

from util import Actor, Event


@ray.remote
class Ship(Actor):
    def __init__(self, name: str, location: str, replay: bool = False):
        self.name: str
        self.location: str
        self.owner: str
        self.cargo: List[str]
        self.log: List[Event] = list()
        if not replay:
            self.on(Ship.Creation(name, location))

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
        return self.log

    @staticmethod
    def replay(events: List[Event]):
        ret = Ship.remote("", "", True)
        for event in events:
            ret.on.remote(event)
        return ret

    class InvalidActionException(Exception):...

    class Creation(Event):
        def __init__(self, ship: str, port: str):
            super().__init__()
            self.ship = ship
            self.port = port

    class TransferOwnership(Event):
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
