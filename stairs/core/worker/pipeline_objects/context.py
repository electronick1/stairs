

class ComponentContext:
    """
    Provide context for component, which data should be transferred and who
    will be responsible for this data in the future.
    """

    def __init__(self, p_component, component_keys_transformation):
        """
        :param p_component:
        :param transformation: key:value dict, where:
            key - data value name which we want to transfer
            value - data value name which should be used in new component
        """
        self.to_p_component = p_component
        self.transformation = component_keys_transformation

    def assign_labels(self, data):
        output_data = dict()

        for key, value in data.items():
            key = "%s->%s" % (key, self.to_p_component.id)
            output_data[key] = value

        return output_data


def is_context_key(key):
    return '->' in key


def belongs_to_component(component, key):
    if '->' in key:
        return component.id in key

    return False


def unassign_data(component, key):
    return key.split('->')[0]
