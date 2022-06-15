class ServiceLocatorError(Exception):
    pass


class ServiceLocator:
    _services = {}

    def bind(self, interface, _class_object):
        assert isinstance(_class_object, interface)
        if interface in self._services:
            raise ServiceLocatorError("Interface {} already bound".format(interface.__name__))

        self._services[interface] = _class_object

    def get(self, interface):
        if interface not in self._services:
            raise ServiceLocatorError("Interface {} was not bound".format(interface.__name__))

        return self._services[interface]


service_locator = ServiceLocator()
