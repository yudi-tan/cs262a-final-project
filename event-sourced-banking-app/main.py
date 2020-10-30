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
        print(ray.get(ref))