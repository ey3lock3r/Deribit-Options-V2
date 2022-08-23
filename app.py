from Bot_V2 import CBot, FILE

import yaml
import logging.config

def main():

    # Подгружаем конфиг
    with open('./config.yaml','r') as f:
        config = yaml.load(f.read(), Loader = yaml.FullLoader)

    logging.addLevelName(FILE,"FILE")
    logging.config.dictConfig(config['logging'])

    bot = CBot(**config['bot'])
    bot.run()

if __name__ == '__main__':
    main()