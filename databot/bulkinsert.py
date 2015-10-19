from databot.utils.objsize import getsize


class BulkInsert(object):
    def __init__(self, engine, table, threshold=1000000):
        self.size = 0
        self.engine = engine
        self.table = table
        self._post_save = None
        self.threshold = threshold
        self.buffer_ = []

    def post_save(self, func):
        self._post_save = func

    def append(self, data):
        size = getsize(data)
        if (self.size + size) > self.threshold:
            self.save()
        self.size += size
        self.buffer_.append(data)

    def save(self, post_save=False):
        if self.buffer_:
            self.engine.execute(self.table.insert(), self.buffer_)
            self.size = 0
            self.buffer_ = []
            if self._post_save:
                self._post_save()
        elif self._post_save and post_save:
            self._post_save()
