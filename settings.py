# coding=<utf-8>
import yaml


class toponymSettings():
    def __init__(self, path='toponym_settings.yaml'):
        """Loading or creating toponym settings file"""
        self.path = path

        try:
            with open(self.path, 'r') as f:
                for key, value in yaml.safe_load(f).items():
                    setattr(self, key, value)
        # TODO: (80) Deal with these exceptions better.
        except FileNotFoundError:
            pass
        except AttributeError:
            pass

    def save(self):
        """Save the settings file"""
        with open(self.path, 'w') as f:
            yaml.safe_dump(self.__dict__, f)


settings = toponymSettings()
