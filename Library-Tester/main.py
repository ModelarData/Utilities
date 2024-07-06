"""Script for testing the ModelarDB Python library."""

import configparser

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")

    personal_access_token = config["DEFAULT"]["GITHUB_PERSONAL_ACCESS_TOKEN"]
    print(personal_access_token)
