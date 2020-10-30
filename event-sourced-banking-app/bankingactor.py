import sqlite3
import ray
import command
import json

@ray.remote
class BankingActor:
    def __init__(self, db_schema):
        # this is the internal global state of the system. Constructured via
        # Event Sourcing.
        # Initialize database connection and replay events to build ledger.
        self.db = sqlite3.connect('banking.db')
        self.__initialize_db(db_schema)
        self.ledger = {}
        self.__replay_events() 
        
    
    # Handles the C in "CQRS".
    # Input: command - command class which encapsulates
    # Output: boolean - whether command succeeded or not. Clients have to wait
    # for this asynchronous call to complete to know if command is successfully processed.
    def process_command(self, command):
        if command.command_type == "DEPOSIT":
            # TODO: validate command first
            # Once validated, we store an event.
            c = self.db.cursor()
            c.execute("INSERT INTO bankevents (event_type, payload) VALUES (?, ?)", [command.command_type, command.payload])
            self.db.commit()
            # update internal global state
            self.__process_event(command.command_type, command.payload)
            return c.rowcount >= 1 # rowcount is 1 is succeeded.

    # Handles the Q in "CQRS"
    # Input: user - string representing the user (username)
    # Output: int - user's balance
    def retrieve_balance(self, user):
        return self.ledger[user]

    # Reads and replays every event to build up the internal self.ledger state.
    # Called during actor initialization
    def __replay_events(self):
        c = self.db.cursor()
        c.execute('SELECT * FROM bankevents') 
        for row in c:
            self.__process_event(row[1], row[2])

    # Internal helper to process each event and update the internal global
    # state.
    # Input: event_type - string 
    # Input: payload - json
    def __process_event(self, event_type, payload):
        if event_type == "DEPOSIT":
            event_dict = json.loads(payload)
            user = event_dict["user"]
            amount = event_dict["amount"]
            if user not in self.ledger:
                self.ledger[user] = 0
            self.ledger[user] += amount
    
    # Initializes the db schema by reading from the text file.
    # Input: db_schema - string
    def __initialize_db(self, db_schema):
        f = open(db_schema, "r")
        schema = f.read()
        c = self.db.cursor()
        c.execute(schema)
        self.db.commit()


