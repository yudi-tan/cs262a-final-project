class Command:
    # command_type: string
    # payload: json
    def __init__(self, user, command_type, payload):
        self.user = user
        self.command_type = command_type
        self.payload = payload
