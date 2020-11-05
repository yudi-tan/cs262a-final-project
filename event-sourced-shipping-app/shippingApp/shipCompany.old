import abc
from typing import List

import ray

from util import Actor, Event, sortStreams
from ship import Ship

@ray.remote
class ShipCompany(Actor): # not done yet
    def __init__(self, name: str, replay: bool = False):
        self.name: str
        self.location: str
        
        # self.ships: List[ray.actor.ActorHandle] = list()
        self.ships: List[Ship] = list()
        self.log: List[Event] = list()
        if not replay:
            self.on(ShipCompany.Establish(name))
    
    def eventHandler(self, event: Event):
        if not isinstance(event, Event):
            return
        elif isinstance(event, ShipCompany.Establish):
            self.name = event.name
        elif isinstance(event, ShipCompany.Acquire):
            self.ships.append(event.ship)
            event.ship.on.remote(Ship.TransferOwnership(self))
        elif isinstance(event, ShipCompany.Unacquire):
            self.ships.remove(event.ship)
            event.ship.on.remote(Ship.TransferOwnership(None))
        else:
            raise NotImplementedError
    
    def on(self, event: Event):
        self.log.append(event)
        self.eventHandler(event)
    
    def acquire(self, ship: Ship):
        if ray.get(ship.getOwner.remote()) is not None:
            raise ShipCompany.InvalidActionException
        self.on(ShipCompany.Acquire(ship))
    
    def unacquire(self, ship: Ship):
        if ray.get(ship.getOwner.remote()) != self:
            raise ShipCompany.InvalidActionException
        self.on(ShipCompany.Unacquire(ship))
    
    def getGlobalLogStream(self) -> List[Event]:
        return sortStreams([self.log] + [ray.get(ship.getLog.remote()) for ship in self.ships])


    class InvalidActionException(Exception): ...

    class Establish(Event):
        # establish of a company
        def __init__(self, name):
            super().__init__()
            self.name = name

    class Acquire(Event):
        # acquiring of a ship
        def __init__(self, ship: Ship):
            super().__init__()
            self.ship = ship
    
    class Unacquire(Event):
        def __init__(self, ship: Ship):
            super().__init__()
            self.ship = ship
