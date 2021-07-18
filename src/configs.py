from configparser import ConfigParser
from os import path, listdir
from typing import Dict


class Configs():
    __dict: Dict[str, Dict[str, ConfigParser]] = None

    @staticmethod
    def configs(category: str, target: str, section: str):
        return lambda option: Configs.__get(category, target, section, option)

    @staticmethod
    def __setup():
        if Configs.__dict is None:
            Configs.__dict = {}
            config_path = path.join(path.dirname(
                path.dirname(path.abspath(__file__))), 'configs')
            for category in listdir(config_path):
                Configs.__dict[category] = {}
                category_path = path.join(config_path, category)
                for target in listdir(category_path):
                    name, ext = path.basename(target).split('.')
                    if ext != "ini":
                        continue
                    cp = ConfigParser()
                    cp.read(path.join(category_path, target))
                    Configs.__dict[category][name] = cp

    @staticmethod
    def __get(category: str, target: str, section: str, option: str):
        Configs.__setup()

        try:
            return Configs.__dict[category][target].get(section, option)
        except:
            return None


if __name__ == "__main__":
    print(Configs.configs("credentials", "mysql", "production")("db"))
