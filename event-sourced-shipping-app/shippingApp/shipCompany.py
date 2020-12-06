import abc
from typing import List

import ray

from util import Actor, Event, handleEQ, sortStreams
from ship import Ship


@ray.remote
class ShipCompany(Actor): # global single-instance implementation
    def __init__(self, replay: bool = False):
        self.ships: Dict[str, List[Ship]] = dict()
        self.log: List[Event] = list()

    def eventHandler(self, event: Event):
        if not isinstance(event, Event):
            return
        elif isinstance(event, ShipCompany.Establish):
            self.ships[event.name] = list()
        elif isinstance(event, ShipCompany.Acquire):
            self.ships[event.company].append(event.ship)
        elif isinstance(event, ShipCompany.Unacquire):
            for ship in self.ships[event.company].copy():
                if handleEQ(ship, event.ship):
                    self.ships[event.company].remove(ship)
                    break
        elif isinstance(event, ShipCompany.Transfer):
            for ship in self.ships[event.oldCompany].copy():
                if handleEQ(ship, event.ship):
                    self.ships[event.oldCompany].remove(ship)
                    break
            self.ships[event.newCompany].append(event.ship)
        else:
            raise NotImplementedError

    def on(self, event: Event):
        self.log.append(event)
        self.eventHandler(event)

    def establish(self, name: str):
        if name in self.ships.keys():
            raise ShipCompany.InvalidActionException
        self.on(ShipCompany.Establish(name))

    def acquire(self, ship: Ship, company: str):
        if ray.get(ship.getOwner.remote()) != "":
            raise ShipCompany.InvalidActionException
        self.on(ShipCompany.Acquire(ship, company))
        ship.on.remote(Ship.TransferOwnership(company))

    def unacquire(self, ship: Ship, company: str):
        if ray.get(ship.getOwner.remote()) != company:
            raise ShipCompany.InvalidActionException
        self.on(ShipCompany.Unacquire(ship, company))
        ship.on.remote(Ship.TransferOwnership(""))

    def transfer(self, ship: Ship, oldCompany: str, newCompany: str):
        if ray.get(ship.getOwner.remote()) != oldCompany:
            raise ShipCompany.InvalidActionException
        if not any([handleEQ(ship, shipIter) for shipIter in self.ships[oldCompany]]):
            raise ShipCompany.InvalidActionException
        if ship._actor_id not in [x._actor_id for x in self.ships[oldCompany]]:
            raise ShipCompany.InvalidActionException
        if newCompany not in self.ships.keys():
            self.on(ShipCompany.Establish(newCompany))
        self.on(ShipCompany.Transfer(ship, oldCompany, newCompany))
        ship.on.remote(Ship.TransferOwnership(newCompany))

    def getState(self):
        return self.ships

    def getGlobalLogStream(self) -> List[Event]:
        streams = [self.log]
        for lst in self.ships.values():
            for ship in lst:
                streams.append(ray.get(ship.getLog.remote()))
        return sortStreams(streams)


    class InvalidActionException(Exception): ...

    class Establish(Event):
        # establish of a company
        def __init__(self, name):
            super().__init__()
            self.name = name

    class Acquire(Event):
        # acquiring of a ship
        def __init__(self, ship: Ship, company: str):
            super().__init__()
            self.ship = ship
            self.company = company

    class Unacquire(Event):
        def __init__(self, ship: Ship, company: str):
            super().__init__()
            self.ship = ship
            self.company = company

    class Transfer(Event):
        def __init__(self, ship, oldCompany, newCompany):
            super().__init__()
            self.ship = ship
            self.oldCompany = oldCompany
            self.newCompany = newCompany
