import json, random, datetime

num_users = 20
num_events = 10000
snapshot_interval = 100
outputFile = f"test{num_events}_{snapshot_interval}.py"

random.seed(0)

def printCmd(user: str, amount: int, command: str, actor: str):
    assert command == "DEPOSIT" or command == "WITHDRAW"
    ret  = f"payload_dict = {{ 'user': '{user}', 'amount': {amount} }}\n"
    ret += "payload = json.dumps(payload_dict)\n"
    ret += f"command = Command('{command}', payload)\n"
    ret += f"{actor}.process_command(command)\n"
    return ret

output = """import json
import os
from command import Command
from bankingactor import *
"""
output += "open('data/log/logfile.txt', 'w').close()\n"
actor = "bank"
output += f"{actor} = BankingActor()\n\n"

bank = {}
for i in range(num_users):
    bank["User" + str(i)] = 0
allUsers = list(bank.keys())

count = 1
offsets = []
validate_offset = 0
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
        validate_offset = random.randint(count, count + 9)
        output += f"{actor}.snapshot()\n"
        offsets.append(count)

    if count == validate_offset:
        # Validate that replay by snapshots is valid
        output += f"replayed = BankingActor()\n"
        output += f"assert {actor}.retrieve_ledger() == replayed.retrieve_ledger()\n"

    count += 1

output += "\n"

for user in allUsers:
    output += f"user_amount = {actor}.retrieve_balance('{user}')\n"
    output += f"assert user_amount == {bank[user]}\n"

output += "\n"

with open(outputFile, "w") as f:
    f.write(output)
