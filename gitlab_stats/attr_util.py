
def set_attributes_for_collection(objects, **fields):
    for obj in objects:
        for key, value in fields.items():
            if callable(value):
                value = value(obj)
            setattr(obj, key, value)
