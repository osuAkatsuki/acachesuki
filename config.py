from typing import Union

from logger import debug
from logger import info

try:
    from orjson import (
        dumps as j_dumps,
        loads as j_loads,
    )
except ImportError:
    from json import (
        dumps as j_dumps,
        loads as j_loads,
    )

import os

__name__ = "ConfigModule"
__author__ = "RealistikDash"
__version__ = "v2.0.0"


class JsonFile:
    """Assists within working with simple JSON files."""

    def __init__(self, file_name: str, load: bool = True):
        """Loads a Json file `file_name` from disk.

        Args:
            file_name (str): The path including the filename of the JSON file
                you would like to load.
            load (str): Whether the JSON file should be loaded immidiately on
                object creation.
        """

        self.file = None
        self.file_name = file_name
        if load and os.path.exists(file_name):
            self.load_file()

    def load_file(self) -> None:
        """Reloads the file fully into memory."""

        with open(self.file_name) as f:
            self.file = j_loads(f.read())

    def get_file(self) -> dict:
        """Returns the loaded JSON file as a dict.

        Returns:
            Contents of the file.
        """
        return self.file

    def write_file(self, new_content: Union[dict, list]) -> None:
        """Writes `new_content` to the target file.

        Args:
            new_content (dict, list): The new content that should be placed
                within the file.
        """

        with open(self.file_name, "w") as f:
            json_content = j_dumps(
                new_content
            ).decode()  # TODO: This doesnt take indent arg?????
            f.write(json_content)

        self.file = new_content


class ConfigReader:
    """A parent class meant for the easy management, updating and the creation
    of a configuration `JSON` file."""

    def __init__(self):
        """Sets placeholder variables."""

        # Set to true if a new value was added to the config.
        self.updated: bool = False
        self.updated_keys: list = []

        # An object around the configuration file.
        self.json: JsonFile = JsonFile("config.json")

    def __init_subclass__(cls, stop_on_update: bool = False):
        """Sets and reads the config child class."""

        cls.__init__(cls)

        # Now we read all of the annotated valiables.
        for var_name, key_type in cls.__annotations__.items():

            # We are checking for a possible default value (in case its a new field)
            default = getattr(cls, var_name, None)

            # Read the key.
            key_val = cls.read_json(cls, var_name, default)

            # Force it to be the sepcified type.
            key_val = key_type(key_val)

            # Set the attribute.
            setattr(cls, var_name, key_val)

        if cls.updated:
            info(
                "The config has just been updated! Please edit according to your preferences!"
            )
            debug("Keys added: " + ", ".join(cls.updated_keys))
            raise SystemExit

    def read_json(self, key: str, default=None):
        """Reads a value directly from the json file and returns
        if. If the value is not already in the JSON file, it adds
        it and sets it as `default`.

        Args:
            key (str): The JSON key to fetch the value of.
            default (any): The value for the key to be set to if the
                value is not set.

        Returns:
            Value of the key.
        """

        # Handle a case where the file is empty/not found.
        if self.json.file is None:
            # Set it to an empty dict so it can be handled with the thing below.
            self.json.file = {}

        # Check if the key is present. If not, set it.
        if key not in tuple(self.json.file):
            # Set it so we can check if the key was modified.
            self.updated = True
            self.updated_keys.append(key)
            # Set the value in dict.
            self.json.file[key] = default

            # Write it to the file.
            self.json.write_file(self.json.file)

            # Return default
            return default

        # It exists, just return it.
        return self.json.file[key]


# TODO: Notifications for config updates.
class Config(ConfigReader):
    """The main class for the storage of config values.
    These values are read directly from the `config.json` file."""

    http_sock: str = "/tmp/acachesuki.sock"
    sql_host: str = "localhost"
    sql_user: str = "root"
    sql_db: str = "ripple"
    sql_password: str = ""

    fokabot_key: str = ""
    osu_api_keys: list[str] = []


conf = Config()
