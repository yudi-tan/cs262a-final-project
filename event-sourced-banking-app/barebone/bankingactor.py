import command
import json
import datetime
import time
import os

snapshots = {}

class BankingActor:
    def __init__(self, db_schema="", snapshot=None):
        self.ledger = {}
        self.log = []
        self.__replay_events(snapshot)
    
    def process_command(self, command):
        if command.command_type == "WITHDRAW":
            event_dict = json.loads(command.payload)
            if event_dict["amount"] > self.ledger.get(event_dict["user"], 0):
                return False
        elif command.command_type != "DEPOSIT":
            return False
        
        self.log.append((command.command_type, command.payload, datetime.date.today().strftime("%Y-%m-%d %H:%M:%S")))
        self.__process_event(command.command_type, command.payload)
        return True
    
    def snapshot(self):
        offset = len(self.log)
        snapshot_dict = {}
        snapshot_dict["data"] = self.ledger
        snapshot_dict["offset"] = offset # placeholder
        snapshot_dict["timestamp"] = datetime.date.today().strftime("%d-%m-%Y %H:%M:%S")
        snapshot = json.dumps(snapshot_dict, indent = 4)

        snapshots[offset] = snapshot
    
    def retrieve_balance(self, user):
        return self.ledger[user]
    
    def retrieve_ledger(self):
        return self.ledger

    def moving_min_deposit(self, start_time, end_time):
        q = filter(lambda e: e[2] >= start_time and e[2] <= end_time, self.log)
        result = float('inf')
        for event in q:
            if event[0] == "DEPOSIT":
                event_dict = json.loads(event[1])
                amount = event_dict["amount"]
                result = min(result, amount)
        if result == float('inf'):
            return 0
        return result
    
    def moving_sum_deposit(self, start_time, end_time):
        q = filter(lambda e: e[2] >= start_time and e[2] <= end_time, self.log)
        total = 0
        for event in q:
            if event[0] == "DEPOSIT":
                event_dict = json.loads(event[1])
                amount = event_dict["amount"]
                total += amount
        return total
    
    def __replay_events(self, snapshot):
        offset = 0
        if snapshot:
            snapshot_dict = json.loads(snapshot)
            self.ledger = snapshot_dict["data"]
            offset = snapshot_dict["offset"]
        for event in self.log[offset:]:
            self.__process_event(event[0], event[1])

    def __process_event(self, event_type, payload):
        event_dict = json.loads(payload)
        user = event_dict["user"]
        amount = event_dict["amount"]
        if event_type == "DEPOSIT":
            self.ledger[user] = self.ledger.get(user, 0) + amount
        elif event_type == "WITHDRAW":
            self.ledger[user] -= amount