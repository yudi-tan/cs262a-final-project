class Command:
    # command_type: string
    # payload: json
    def __init__(self, command_type, payload):
        self.command_type = command_type
        self.payload = payload