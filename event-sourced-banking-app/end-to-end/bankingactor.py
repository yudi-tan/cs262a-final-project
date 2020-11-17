import sqlite3
import ray
import command
import json
import datetime
import os

@ray.remote
class BankingActor:
    def __init__(self, db_schema, snapshot=None):
        # this is the internal global state of the system. Constructured via
        # Event Sourcing.
        # Initialize database connection and replay events to build ledger.
        pathname = 'data/banking.db'
        os.makedirs(os.path.dirname(pathname), exist_ok=True)
        self.db = sqlite3.connect(pathname)
        self.__initialize_db(db_schema)
        self.ledger = {}
        self.__replay_events(snapshot)

    # Handles the C in "CQRS".
    # Input: command - command class which encapsulates
    # Output: boolean - whether command succeeded or not. Clients have to wait
    # for this asynchronous call to complete to know if command is successfully processed.
    def process_command(self, command):
        # TODO: validate command first
        if command.command_type == "WITHDRAW":
            event_dict = json.loads(command.payload)
            if event_dict["amount"] > self.ledger.get(event_dict["user"], 0):
                return False
        elif command.command_type != "DEPOSIT":
            return False

        # Store event
        c = self.db.cursor()
        c.execute("INSERT INTO bankevents (event_type, payload) VALUES (?, ?)", [command.command_type, command.payload])
        self.db.commit()
        # update internal global state
        self.__process_event(command.command_type, command.payload)
        return c.rowcount >= 1 # rowcount is 1 is succeeded.

    # Create a snapshot of the current state
    def snapshot(self):
        c = self.db.cursor()
        c.execute("SELECT last_insert_rowid()")
        offset = next(c)[0]

        snapshot_dict = {}
        snapshot_dict["data"] = self.ledger
        snapshot_dict["offset"] = offset
        snapshot_dict["timestamp"] = datetime.date.today().strftime("%d-%m-%Y %H:%M:%S")
        snapshot = json.dumps(snapshot_dict, indent = 4)

        pathname = f'data/snapshots/{offset}.json'
        os.makedirs(os.path.dirname(pathname), exist_ok=True)
        with open(pathname, 'w') as f:
            json.dump(snapshot, f)

    # Handles the Q in "CQRS"
    # Input: user - string representing the user (username)
    # Output: int - user's balance
    def retrieve_balance(self, user):
        return self.ledger[user]

    def retrieve_ledger(self):
        return self.ledger

    # Handles the Q in "CQRS". More complicated query (i.e. filtering).
    # Input: start_time string, end_time string (in SQL timestamp format)
    # Output: int - minimum deposit within a time window
    def moving_min_deposit(self, start_time, end_time):
        c = self.db.cursor()
        c.execute('SELECT * FROM bankevents WHERE timestamp <= ? and timestamp >= ?', [end_time, start_time])
        result = float('inf')
        for row in c:
            if row[1] == "DEPOSIT":
                event_dict = json.loads(row[2])
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
            if row[1] == "DEPOSIT":
                event_dict = json.loads(row[2])
                amount = event_dict["amount"]
                total += amount
        return total

    # Reads and replays every event to build up the internal self.ledger state.
    # Called during actor initialization
    def __replay_events(self, snapshot):
        offset = 0
        if snapshot:
            snapshot_dict = json.loads(snapshot)
            self.ledger = snapshot_dict["data"]
            offset = snapshot_dict["offset"]
        c = self.db.cursor()
        c.execute('SELECT * FROM bankevents WHERE id > ?', [offset])
        for row in c:
            self.__process_event(row[1], row[2])

    # Internal helper to process each event and update the internal global
    # state.
    # Input: event_type - string
    # Input: payload - json
    def __process_event(self, event_type, payload):
        event_dict = json.loads(payload)
        user = event_dict["user"]
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
