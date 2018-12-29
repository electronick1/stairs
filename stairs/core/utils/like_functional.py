import copy


class LikeFunctional:
    copy_on = []

    def __getattribute__(self, name):
        if name != 'copy_on' and name in self.copy_on:
            new_object = copy.copy(self)
        else:
            new_object = self

        return object.__getattribute__(new_object, name)

