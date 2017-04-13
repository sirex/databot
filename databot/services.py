def merge_rows(rows):
    key = None
    value = None
    merged = False
    for row in rows:
        if row.key == key:
            if isinstance(value, dict) and isinstance(row.value, dict):
                value.update(row.value)
                merged = True
            else:
                value = row.value
        else:
            if merged:
                merged = False
                yield key, value
            key = row.key
            value = row.value
