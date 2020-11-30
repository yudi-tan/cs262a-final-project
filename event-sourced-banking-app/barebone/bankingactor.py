import command
import json
import datetime
import time
import os

SNAPSHOTS_DIR = 'data/snapshots'
LOG_DIR = 'data/log'

class BankingActor:
    def __init__(self):
        self.ledger = {} 
        self.offset = -1
        # on initialization, we load the latest snapshot and update internal state.
        if os.path.exists(SNAPSHOTS_DIR):
            snapshots = os.listdir(SNAPSHOTS_DIR)
            if snapshots:
                with open(os.path.join(SNAPSHOTS_DIR, max(snapshots)), 'r') as f:
                    snapshot_dict = json.load(f)
                self.ledger = snapshot_dict["data"]
                self.offset = snapshot_dict["offset"]
                f.close()
        # create logfile if not exist
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)
            open(LOG_DIR + "/logfile.txt", "w").close()
        
    
    def process_command(self, command):
        # basic command validation.
        if command.command_type == "WITHDRAW":
            event_dict = json.loads(command.payload)
            curr_balance = self.retrieve_balance(event_dict["user"])
            if event_dict["amount"] > curr_balance:
                return False
        elif command.command_type != "DEPOSIT":
            return False

        # Store event in WAL.
        log_entry = {}
        log_entry["command_type"] = command.command_type
        log_entry["payload"] = command.payload
        log_entry["timestamp"] = datetime.date.today().strftime("%Y-%m-%d %H:%M:%S")
        log_str = json.dumps(log_entry) + "\n"
        logfile = open(LOG_DIR + "/logfile.txt", "a")
        logfile.write(log_str)
        logfile.close()
        return True
    
    # Create a snapshot of the current state
    def snapshot(self):
        # update the internal state to include the latest entries
        self.__replay_events()
        # once internal state is up-to-date, we create the snapshot entry.
        snapshot_dict = {}
        snapshot_dict["data"] = self.ledger
        snapshot_dict["offset"] = self.offset
        snapshot_dict["timestamp"] = datetime.date.today().strftime("%d-%m-%Y %H:%M:%S")

        pathname = os.path.join(SNAPSHOTS_DIR, f'{self.offset}.json')
        os.makedirs(os.path.dirname(pathname), exist_ok=True)
        with open(pathname, 'w') as f:
            json.dump(snapshot_dict, f, indent = 4)
        f.close()
    
    # Handles the Q in "CQRS"
    # Input: user - string representing the user (username)
    # Output: int - user's balance
    def retrieve_balance(self, user):
        # read from latest snapshot (in-memory)
        cur_balance = self.ledger.get(user, 0)
        # using the offset, retrieve all relevant events which occured after
        # snapshot
        logfile = open(LOG_DIR + "/logfile.txt", "r")
        for line in logfile.readlines()[self.offset+1:]:
            if not line:
                break
            log_entry = json.loads(line)
            payload = json.loads(log_entry["payload"])
            if payload["user"] == user:
                if log_entry["command_type"] == "WITHDRAW":
                    cur_balance -= payload["amount"]
                elif log_entry["command_type"] == "DEPOSIT":
                    cur_balance += payload["amount"]
        logfile.close()
        return cur_balance


    # Builds and retrieves the latest ledger state
    def retrieve_ledger(self):
        self.__replay_events()
        return self.ledger

    # updates the internal states to reflect the latest entries.
    def __replay_events(self):
        logfile = open(LOG_DIR + "/logfile.txt", "r")
        for i, line in enumerate(logfile.readlines()):
            if i <= self.offset:
                continue
            if not line:
                break
            log_entry = json.loads(line)
            payload = json.loads(log_entry["payload"])
            if payload["user"] not in self.ledger:
                self.ledger[payload["user"]] = 0
            if log_entry["command_type"] == "WITHDRAW":
                self.ledger[payload["user"]] -= payload["amount"]
            elif log_entry["command_type"] == "DEPOSIT":
                self.ledger[payload["user"]] += payload["amount"]
            self.offset = i
        logfile.close()
        return