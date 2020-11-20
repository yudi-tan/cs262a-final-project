import json, random, datetime

num_users = 20
num_events = 1000
snapshot_interval = 100
outputFile = f"test{num_events}.py"

# random.seed(datetime.datetime.now())
random.seed(0)

def printCmd(user: str, amount: int, command: str, actor: str):
    assert command == "DEPOSIT" or command == "WITHDRAW"
    ret  = f"payload_dict = {{ 'user': '{user}', 'amount': {amount} }}\n"
    ret += "payload = json.dumps(payload_dict)\n"
    ret += f"command = Command('{command}', payload)\n"
    ret += f"{actor}.process_command(command)\n"
    return ret

output = """import json
import ray
import os
from command import Command
from bankingactor import *

"""
actor = "bank"
output += f"{actor} = BankingActor('./BankEventsSchema.txt')\n\n"

bank = {}
for i in range(num_users):
    bank["User" + str(i)] = 0
allUsers = list(bank.keys())

count = 1
offsets = []
while count < num_events:
    user = random.choice(allUsers)
    command = random.choice(["DEPOSIT", "WITHDRAW"])
    amount = random.randint(1, 1000)
    if command == "DEPOSIT":
        bank[user] += amount
    elif command == "WITHDRAW":
        if amount > bank[user]:
            continue
        bank[user] -= amount
    output += printCmd(user, amount, command, actor)

    if count % snapshot_interval == 0:
        output += f"{actor}.snapshot()\n"
        offsets.append(count)

    count += 1

output += "\n"

for user in allUsers:
    output += f"user_amount = {actor}.retrieve_balance('{user}')\n"
    output += f"assert user_amount == {bank[user]}\n"

output += "\n"

# Validate that recovery using snapshots is valid
offset = random.choice(offsets)
# output += f"with open('data/snapshots/{offset}.json', 'r') as f:\n"
output += f"replayed = BankingActor('./BankEventsSchema.txt', snapshots[{offset}])\n"
output += f"{actor}.retrieve_ledger() == replayed.retrieve_ledger()\n"

with open(outputFile, "w") as f:
    f.write(output)
