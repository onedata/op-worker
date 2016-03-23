import imp
import os


class PluginsLoader:
    """Loads generators (all python files) from 'generators' directory.
    Each generator will be available under key created from its filename.
    """

    def __init__(self):
        self.plugins = {}

    def load_plugins(self):
        plugins = [f.split('.')[0] for f in os.listdir("generators") if
                   f.endswith('.py')]
        for p in plugins:
            self.plugins[p] = imp.load_module(p, *imp.find_module(p, [
                'generators']))

    def get_plugin(self, name):
        return self.plugins[name]

    def get_available_plugins(self):
        return self.plugins.keys()
