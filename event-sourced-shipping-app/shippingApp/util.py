import abc, heapq, time
from typing import Any, List

class Actor(abc.ABC): ...
class Event(abc.ABC):
    def __init__(self):
        self.happened = time.time()

    def __repr__(self):
        return f"{self.__class__.__qualname__} event: {self.__dict__}"

# I think we can also make this a remote function?
def sortStreams(streams: List[List[Any]], key=lambda x: x.happened) -> List[Any]:
    flattened = []
    for stream in streams:
        for log in stream:
            flattened.append((key(log), log)) # stackoverflow.com/questions/8875706/
    heapq.heapify(flattened)
    return [item[1] for item in flattened]

def handleEQ(handle1, handle2):
    # two handles that stem from the same Actor will not be equal via ==, therefore
    # we are having this function.
    return handle1 == handle2 or str(handle1) == str(handle2)
    # the str will have the _actor_id embedded. We won't break abstraction barrier then
