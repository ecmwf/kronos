#!/usr/bin/env python
import json
import os
import sys

from kronos_io.schema_description import SchemaDescription

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# from kronos_io.profile_format import ProfileFormat


if __name__ == '__main__':

    config_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

    with open(os.path.join(config_dir, "config/config_schema.json"), "r") as json_file:
        conf_data = json.load(json_file)

    print SchemaDescription.from_schema(conf_data)




