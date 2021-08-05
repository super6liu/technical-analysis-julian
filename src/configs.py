from json import load


with open('configs.json') as f:
    CONFIGS = load(f)

with open('credentials.json') as f:
    CREDENTIALS = load(f)
