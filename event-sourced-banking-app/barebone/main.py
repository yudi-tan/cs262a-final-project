import os
import shutil
from command import Command
import json
from bankingactor import *

def add_command(type, user, amount):
    payload_dict = {}
    payload_dict["user"] = user
    payload_dict["amount"] = amount
    payload = json.dumps(payload_dict, indent = 4)
    command = Command(type, payload)
    res = a.process_command(command)
    if not res:
        print(f"{type} command failed")

if __name__ == "__main__":
    a = BankingActor("./BankEventsSchema.txt")
    add_command("DEPOSIT", "alice", 100)
    add_command("WITHDRAW", "alice", 50)

    a.snapshot()
    alice = a.retrieve_balance("alice")
    print("Balance: ", alice)
    res = a.moving_min_deposit('2007-01-01 10:00:00', '2077-01-01 10:00:00')
    print("Moving min: ", res)
    res = a.moving_sum_deposit('2007-01-01 10:00:00', '2077-01-01 10:00:00')
    print("Moving sum: ", res)

    add_command("WITHDRAW", "alice", 25)
    add_command("DEPOSIT", "bob", 200)
    # should fail and not get recorded
    add_command("WITHDRAW", "alice", 100)

    a.snapshot()
    res = a.retrieve_balance("alice")
    print("alice Balance: ", res)
    res = a.retrieve_balance("bob")
    print("bob Balance: ", res)

    b = BankingActor("./BankEventsSchema.txt", snapshots[2])

    print("Recovered from snapshot:")
    res = a.retrieve_balance("alice")
    print("alice Balance: ", res)
    res = a.retrieve_balance("bob")
    print("bob Balance: ", res)