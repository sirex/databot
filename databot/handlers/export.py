def handler(path):
    if path.endswith('.csv'):
        import databot.exporters.csv
        return databot.exporters.csv.Exporter(path)
    else:
        raise ValueError("Don't know how to export %s." % path)
