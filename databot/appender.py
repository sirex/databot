class Appender(object):
    def __init__(self, threshold=100):
        self.data = []
        self.threshold = threshold
        self.offset = 0
        self.receivers = []

    def append(self, value):
        size = self.size(value)
        if self.offset + size > self.threshold:
            self.save()
        self.data.append(value)
        self.offset += size

    def size(self, value):
        return 1

    def watch(self, *appenders):
        for appender in appenders:
            appender.receivers.append(self)
            self.receivers.append(appender)

    def save(self, refs=()):
        if id(self) not in refs:
            self.commit()
            for receiver in self.receivers:
                receiver.save(refs=refs + (id(self),))

            self.data = []
            self.offset = 0

    def commit(self):
        pass
