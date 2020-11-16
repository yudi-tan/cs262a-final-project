import json, random

num_users = 20
num_events = 1000
outputFile = f"test{num_events}.py"

random.seed(0)

openBracket = '{'
closeBracket = '}'

def printCmd(user: str, amount: int, command: str, actor: str):
    assert command == "DEPOSIT" or command == "WITHDRAW"
    ret  = f"payload_dict = {openBracket} 'user': '{user}', 'amount': {amount} {closeBracket}\n"
    ret += "payload = json.dumps(payload_dict)\n"
    ret += f"command = Command('{command}', payload)\n"
    ret += f"{actor}.process_command.remote(command)\n"
    return ret

output = """import json
import ray
import os
import shutil
from command import Command
from bankingactor import BankingActor

if os.path.exists('data/banking.db'):
    os.remove('data/banking.db')
if os.path.exists('data/snapshots'):
    shutil.rmtree('data/snapshots/')

ray.init()
"""
actor = "bank"
output += f"{actor} = BankingActor.remote('./BankEventsSchema.txt')\n\n"

bank = {}
for i in range(num_users):
    bank["User" + str(i)] = 0
allUsers = list(bank.keys())

count = 0
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

    if count % 100 == 0:
        output += f"{actor}.snapshot.remote()\n"

    count += 1

output += "\n"

for user in allUsers:
    output += f"ref = {actor}.retrieve_balance.remote('{user}')\n"
    output += f"user_amount = ray.get(ref)\n"
    output += f"assert user_amount == {bank[user]}\n"

with open(outputFile, "w") as f:
    f.write(output)
