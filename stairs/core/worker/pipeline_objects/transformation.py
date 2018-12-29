

class KeyToKey:

    def __init__(self, transformation_map: dict):
        self.transformation_map = transformation_map

    def __call__(self, data_to_transform):
        output_data = dict()

        for key, value in data_to_transform.items():
            new_key = self.transform_and_assign(key)
            if new_key is not None:
                output_data[new_key] = value

        return output_data

    def transform_and_assign(self, key):

        if not self.transformation_map:
            return key

        if key in self.transformation_map:
            new_key = self.transformation_map[key]
            return new_key


class KeysToDict:
    def __init__(self, dict_key, transformation_map):
        self.dict_key = dict_key
        self.transformation_map = transformation_map
        self.key_to_key = KeyToKey(transformation_map)

    def __call__(self, data_to_transform):
        if self.transformation_map:
            return {self.dict_key: self.key_to_key(data_to_transform)}
        else:
            return {self.dict_key: data_to_transform}


class AllKeys:
    def __init__(self, transformation_map):
        self.transformation_map = transformation_map
        self.key_to_key = KeyToKey(transformation_map)

    def __call__(self, data_to_transform):
        if self.transformation_map:
            return self.key_to_key(data_to_transform)
        else:
            return data_to_transform