from src.configs import CONFIGS
from src.constants import Env


def get_env():
    return Env(CONFIGS["env"] or Env.PRODUCETION)


def set_env(env: Env):
    CONFIGS["env"] = env


def is_test():
    env = get_env()
    return env == Env.TEST or env == Env.DEVELOPMENT
