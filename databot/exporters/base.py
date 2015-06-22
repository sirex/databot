class BaseExporter(object):
    rows = True
    store = False

    def __call__(self, overwrite=False):
        self.overwrite = True
