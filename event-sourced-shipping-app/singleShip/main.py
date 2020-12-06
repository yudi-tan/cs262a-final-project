import abc
from typing import List

import ray

class Actor(abc.ABC): ...
class Event(abc.ABC):
    def __eq__(self, other):
        return type(self) == type(other) and self.__dict__ == other.__dict__

@ray.remote
class Ship(Actor):
    def __init__(self, name: str, location: str, replay: bool = False):
        self.name: str
        self.location: str
        self.cargo: List[str] = list()
        self.log: List[Event] = list()
        if not replay:
            self.on(Creation(name, location))

    def eventHandler(self, event: Event):
        if not isinstance(event, Event):
            return
        elif isinstance(event, Creation):
            self.name = event.ship
            self.location = event.port
        elif isinstance(event, Departure):
            self.location = "SEA"
        elif isinstance(event, Arrival):
            self.location = event.port
        elif isinstance(event, Load):
            self.cargo.append(event.cargo)
        elif isinstance(event, Unload):
            self.cargo.remove(event.cargo)
        else:
            raise NotImplementedError

    def on(self, event: Event):
        self.log.append(event)
        self.eventHandler(event)

    def depart(self, origin: str):
        if self.location == "SEA" or self.location != origin:
            raise
            return
        self.on(Departure(self.name, origin))

    def arrive(self, target: str):
        if self.location != "SEA":
            raise
            return
        self.on(Arrival(self.name, target))

    def load(self, cargo: str):
        if self.location == "SEA":
            raise
            return
        self.on(Load(self.name, cargo))

    def unload(self, cargo: str):
        if self.location == "SEA" or cargo not in self.cargo:
            raise
            return
        self.on(Unload(self.name, cargo))

    def getLog(self):
        return self.log

    def getState(self):
        return self.name, self.location, self.cargo

    @staticmethod
    def replay(events: List[Event]):
        ret = Ship.remote("", "", replay=True)
        for event in events:
            ret.on.remote(event)
        return ret

class Creation(Event):
    def __init__(self, ship: str, port: str):
        self.ship = ship
        self.port = port

class Departure(Event):
    def __init__(self, ship: str, port: str):
        self.ship = ship
        self.port = port

class Arrival(Event):
    def __init__(self, ship: str, port: str):
        self.ship = ship
        self.port = port

class Load(Event):
    def __init__(self, ship: str, cargo: str):
        self.ship = ship
        self.cargo = cargo

class Unload(Event):
    def __init__(self, ship: str, cargo: str):
        self.ship = ship
        self.cargo = cargo
