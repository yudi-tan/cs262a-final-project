import abc, heapq, time
from typing import Any, List

class Actor(abc.ABC): ...
class Event(abc.ABC):
    def __init__(self):
        self.happened = time.time()

# I think we can also make this a remote function?
def sortStreams(streams: List[List[Any]], key=lambda x: x.happened) -> List[Any]:
    flattened = []
    for stream in streams:
        for log in stream:
            flattened.append((key(log), log)) # stackoverflow.com/questions/8875706/
    # flattened = [(key(log), log) for stream in streams for log in stream]
    heapq.heapify(flattened)
    return [item[1] for item in flattened]