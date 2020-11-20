import sqlite3
import ray
import command
import json
import datetime
import os

SNAPSHOTS_DIR = 'data/snapshots'

@ray.remote
class BankingActor:
    def __init__(self, db_schema):
        # this is the internal global state of the system. Constructured via
        # Event Sourcing.
        # Initialize database connection and replay events to build ledger.
        pathname = 'data/banking.db'
        os.makedirs(os.path.dirname(pathname), exist_ok=True)
        self.db = sqlite3.connect(pathname)
        self.__initialize_db(db_schema)

        self.ledger = {}
        self.offset = 0
        if os.path.exists(SNAPSHOTS_DIR):
            snapshots = os.listdir(SNAPSHOTS_DIR)
            if snapshots:
                with open(os.path.join(SNAPSHOTS_DIR, max(snapshots)), 'r') as f:
                    snapshot_dict = json.load(f)
                self.ledger = snapshot_dict["data"]
                self.offset = snapshot_dict["offset"]

    # Handles the C in "CQRS".
    # Input: command - command class which encapsulates
    # Output: boolean - whether command succeeded or not. Clients have to wait
    # for this asynchronous call to complete to know if command is successfully processed.
    def process_command(self, command):
        # TODO: validate command first
        if command.command_type == "WITHDRAW":
            event_dict = json.loads(command.payload)
            if event_dict["amount"] > self.retrieve_balance(command.user):
                return False
        elif command.command_type != "DEPOSIT":
            return False

        # Store event
        c = self.db.cursor()
        c.execute("INSERT INTO bankevents (user, event_type, payload) VALUES (?, ?, ?)", [command.user, command.command_type, command.payload])
        self.db.commit()
        return c.rowcount >= 1 # rowcount is 1 is succeeded.

    # Create a snapshot of the current state
    def snapshot(self):
        self.__replay_events()

        snapshot_dict = {}
        snapshot_dict["data"] = self.ledger
        snapshot_dict["offset"] = self.offset
        snapshot_dict["timestamp"] = datetime.date.today().strftime("%d-%m-%Y %H:%M:%S")

        pathname = os.path.join(SNAPSHOTS_DIR, f'{self.offset}.json')
        os.makedirs(os.path.dirname(pathname), exist_ok=True)
        with open(pathname, 'w') as f:
            json.dump(snapshot_dict, f, indent = 4)

    # Handles the Q in "CQRS"
    # Input: user - string representing the user (username)
    # Output: int - user's balance
    def retrieve_balance(self, user):
        c = self.db.cursor()
        c.execute('SELECT * FROM bankevents WHERE id > ? and user = ?', [self.offset, user])
        cur_balance = self.ledger.get(user, 0)
        for row in c:
            event_type = row[2]
            event_dict = json.loads(row[3])
            amount = event_dict["amount"]
            if event_type == "DEPOSIT":
                cur_balance += amount
            elif event_type == "WITHDRAW":
                cur_balance -= amount
        return cur_balance

    # Builds and retrieves the ledger state
    def retrieve_ledger(self):
        self.__replay_events()
        return self.ledger

    # Handles the Q in "CQRS". More complicated query (i.e. filtering).
    # Input: start_time string, end_time string (in SQL timestamp format)
    # Output: int - minimum deposit within a time window
    def moving_min_deposit(self, start_time, end_time):
        c = self.db.cursor()
        c.execute('SELECT * FROM bankevents WHERE timestamp <= ? and timestamp >= ?', [end_time, start_time])
        result = float('inf')
        for row in c:
            if row[2] == "DEPOSIT":
                event_dict = json.loads(row[3])
                amount = event_dict["amount"]
                result = min(result, amount)
        if result == float('inf'):
            return 0
        return result

    # Handles the Q in "CQRS". More complicated query (i.e. filtering).
    # Input: start_time string, end_time string (in SQL timestamp format)
    # Output: int - total deposit within a time window
    def moving_sum_deposit(self, start_time, end_time):
        c = self.db.cursor()
        c.execute('SELECT * FROM bankevents WHERE timestamp <= ? and timestamp >= ?', [end_time, start_time])
        total = 0
        for row in c:
            if row[2] == "DEPOSIT":
                event_dict = json.loads(row[3])
                amount = event_dict["amount"]
                total += amount
        return total

    # Reads and replays every event to build up the internal self.ledger state.
    # Called during actor initialization
    def __replay_events(self):
        c = self.db.cursor()
        c.execute('SELECT * FROM bankevents WHERE id > ?', [self.offset])
        for row in c:
            self.__process_event(row)

    # Internal helper to process each event and update the internal global
    # state.
    # Input: event_type - string
    # Input: payload - json
    # We could probably remove this method; it's only called once
    def __process_event(self, row):
        self.offset += 1
        assert self.offset == row[0]
        user = row[1]
        event_type = row[2]
        event_dict = json.loads(row[3])
        amount = event_dict["amount"]
        if event_type == "DEPOSIT":
            self.ledger[user] = self.ledger.get(user, 0) + amount
        elif event_type == "WITHDRAW":
            self.ledger[user] -= amount

    # Initializes the db schema by reading from the text file.
    # Input: db_schema - string
    def __initialize_db(self, db_schema):
        with open(db_schema, "r") as f:
            schema = f.read()
        c = self.db.cursor()
        c.execute(schema)
        self.db.commit()
