from flask import Flask, request, jsonify, Response
import ray
import os
import shutil
from command import Command
import json
from bankingactor import BankingActor

app = Flask(__name__)

# Queries the balance for a specific user.
@app.route('/balance', methods=["GET"])
def balance_handler():
    user_id = request.args.get('userid')
    if not user_id:
        return jsonify({})
    response = {}
    response["userid"] = user_id
    ref = a.retrieve_balance.remote(user_id)
    response["balance"] = ray.get(ref)
    return jsonify(response)

# Creates a command / event for a deposit.
# POST request handler - requires a FORM data containing keys (user, amount).
@app.route('/deposit', methods=["POST"])
def deposit_handler():
    form = request.form
    payload_dict = {}
    payload_dict["user"] = form.get('user')
    payload_dict["amount"] = int(form.get("amount"))
    payload = json.dumps(payload_dict, indent = 4)
    command = Command("DEPOSIT", payload)
    ref = a.process_command.remote(command)
    if not ray.get(ref):
        return Response(status=500)
    return Response(status=200)

# Creates a command / event for a withdrawal.
# POST request handler - requires a FORM data containing keys (user, amount).
@app.route('/withdrawal', methods=["POST"])
def withdrawal_handler():
    form = request.form
    payload_dict = {}
    payload_dict["user"] = form.get('user')
    payload_dict["amount"] = int(form.get("amount"))
    payload = json.dumps(payload_dict, indent = 4)
    command = Command("WITHDRAW", payload)
    ref = a.process_command.remote(command)
    if not ray.get(ref):
        return Response(status=500)
    return Response(status=200)


# Helper to create Banking Actor Commands.
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
    # Remove any existing data otherwise tests break
    if os.path.exists('data/banking.db'):
        os.remove('data/banking.db')
    if os.path.exists('data/snapshots'):
        shutil.rmtree('data/snapshots/')

    # initialize ray and mock some data
    ray.init()

    a = BankingActor.remote("./BankEventsSchema.txt")
    add_command("DEPOSIT", "alice", 100)
    add_command("WITHDRAW", "alice", 50)
    ref = a.retrieve_balance.remote("alice")
    print("Balance: ", ray.get(ref))

    # start the server
    app.run(debug=True, port=5000) #run app in debug mode on port 5000