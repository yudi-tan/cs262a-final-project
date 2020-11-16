import ray
from command import Command
import json
from bankingactor import BankingActor

if __name__ == "__main__":
    ray.init()
    a = BankingActor.remote("./BankEventsSchema.txt")
    payload_dict = {}
    payload_dict["user"] = "alice"
    payload_dict["amount"] = 100
    payload = json.dumps(payload_dict, indent = 4)   
    command = Command("DEPOSIT", payload)
    ref = a.process_command.remote(command)
    if not ray.get(ref):
        print("Deposit command failed.")
    else:
        ref = a.retrieve_balance.remote("alice")
        print("Balance: ", ray.get(ref))
        ref = a.moving_min_deposit.remote('2007-01-01 10:00:00', '2077-01-01 10:00:00')
        print("Moving min: ",ray.get(ref))
        ref = a.moving_sum_deposit.remote('2007-01-01 10:00:00', '2077-01-01 10:00:00')
        print("Moving sum: ",ray.get(ref))