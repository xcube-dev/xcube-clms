import threading

from xcube_clms.constants import NOT_STARTED, STARTED, FINISHED


class Event:
    def __init__(self):
        self.event = threading.Event()
        self.state = NOT_STARTED

    def set(self):
        self.event.set()
        self.state = STARTED

    def clear(self):
        self.event.clear()
        self.state = FINISHED

    def reset(self):
        self.event.clear()
        self.state = NOT_STARTED

    def get_state(self):
        return self.state
