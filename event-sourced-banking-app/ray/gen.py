import json, random, datetime

num_users = 20
num_events = 100000
snapshot_interval = 100
outputFile = f"test{num_events}_{snapshot_interval}.py"

# random.seed(datetime.datetime.now())
random.seed(0)

def printCmd(user: str, amount: int, command: str, actor: str):
    assert command == "DEPOSIT" or command == "WITHDRAW"
    ret  = f"payload_dict = {{ 'user': '{user}', 'amount': {amount} }}\n"
    ret += "payload = json.dumps(payload_dict)\n"
    ret += f"command = Command('{user}', '{command}', payload)\n"
    ret += f"{actor}.process_command.remote(command)\n"
    return ret

output = """import json
import ray
import os
import shutil
import time
from command import Command
from bankingactor import BankingActor

if os.path.exists('data/banking.db'):
    os.remove('data/banking.db')
if os.path.exists('data/snapshots'):
    shutil.rmtree('data/snapshots/')

ray.init()
t0 = time.time()
"""
actor = "bank"
output += f"{actor} = BankingActor.remote('./BankEventsSchema.txt')\n\n"

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
        output += f"{actor}.snapshot.remote()\n"
        offsets.append(count)

    if count == validate_offset:
        # Validate that replay by snapshots is valid
        output += f"replayed = BankingActor.remote('./BankEventsSchema.txt')\n"
        output += f"assert ray.get({actor}.retrieve_ledger.remote()) == ray.get(replayed.retrieve_ledger.remote())\n"

    count += 1

output += "\n"

for user in allUsers:
    output += f"ref = {actor}.retrieve_balance.remote('{user}')\n"
    output += f"user_amount = ray.get(ref)\n"
    output += f"assert user_amount == {bank[user]}\n"

output += "t1 = time.time()\n"
output += "print(t1 - t0)\n"

with open(outputFile, "w") as f:
    f.write(output)
