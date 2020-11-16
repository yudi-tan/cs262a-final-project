import ray
from command import Command
import json
from bankingactor import BankingActor

def add_command(type, user, amount):
    payload_dict = {}
    payload_dict["user"] = user
    payload_dict["amount"] = amount
    payload = json.dumps(payload_dict, indent = 4)
    command = Command(type, payload)
    ref = a.process_command.remote(command)
    if not ray.get(ref):
        print(f"{type} command failed")

if __name__ == "__main__":
    ray.init()

    a = BankingActor.remote("./BankEventsSchema.txt")
    add_command("DEPOSIT", "alice", 100)
    add_command("WITHDRAW", "alice", 50)

    a.snapshot.remote()
    ref = a.retrieve_balance.remote("alice")
    print("Balance: ", ray.get(ref))
    ref = a.moving_min_deposit.remote('2007-01-01 10:00:00', '2077-01-01 10:00:00')
    print("Moving min: ",ray.get(ref))
    ref = a.moving_sum_deposit.remote('2007-01-01 10:00:00', '2077-01-01 10:00:00')
    print("Moving sum: ",ray.get(ref))

    add_command("WITHDRAW", "alice", 25)
    add_command("DEPOSIT", "bob", 200)
    # should fail and not get recorded
    add_command("WITHDRAW", "alice", 100)

    a.snapshot.remote()
    ref = a.retrieve_balance.remote("alice")
    print("alice Balance: ", ray.get(ref))
    ref = a.retrieve_balance.remote("bob")
    print("bob Balance: ", ray.get(ref))
