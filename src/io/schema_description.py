

class SchemaDescription(object):
    """
    Provide a description of a JSON schema for user consumption
    """

    @staticmethod
    def from_schema(schema, depth=0):

        if 'enum' in schema:
            return EnumSchemaDescription(schema, depth)

        if isinstance(schema['type'], list):
            return MultiTypeSchemaDescription(schema, depth)

        return SchemaDescription.type_lookup()[schema['type']](schema, depth)

    def __init__(self, schema, depth):
        self._description = schema.get("description", "(none)")
        self.title = schema.get("title", None)
        self.required = schema.get("required", False)
        self.depth = depth

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __unicode__(self):
        s = ""
        if self.title:
            s += "{}\n{}\n".format(self.title, ("-" * len(self.title)))
        s += "# {}\n".format(self.description)
        s += self.details_string()
        if self.title:
            s += "\n--------------------------------------------------"
        return s

    @property
    def newline(self):
        return '\n' + '    ' * self.depth

    @property
    def description(self):
        return "{}{}".format("(REQUIRED) " if self.required else "", self._description)

    @staticmethod
    def type_lookup():
        # This is a method so that the types are defined when it is executed
        return {
            "object": ObjectSchemaDescription,
            "array": ArraySchemaDescription,
            "number": NumberSchemaDescription,
            "string": StringSchemaDescription,
            "integer": IntegerSchemaDescription
        }

    def details_string(self):
        raise NotImplementedError


class ObjectSchemaDescription(SchemaDescription):

    def __init__(self, schema, depth):
        super(ObjectSchemaDescription, self).__init__(schema, depth)

        self.properties = {
            k: SchemaDescription.from_schema(v, depth+1)
            for k, v in schema.get('properties', {}).iteritems()
        }

    def details_string(self):
        if len(self.properties) == 0:
            properties = ""
        else:
            properties = self.newline
            properties += ',{0}{0}'.format(self.newline).join(
                '    # {}{}    {}: {}'.format(prop_schema.description, self.newline, prop, prop_schema.details_string())
                for prop, prop_schema in self.properties.iteritems())
            properties += self.newline

        return "{{{}}}".format(properties)


class ArraySchemaDescription(SchemaDescription):

    def __init__(self, schema, depth):
        super(ArraySchemaDescription, self).__init__(schema, depth)

        if schema.get('items', None) is not None:
            self.items = SchemaDescription.from_schema(schema['items'], depth+1)
        else:
            self.items = None

    def details_string(self):
        if self.items is not None:
            item = self.items.details_string()
            return "[{}, ...]".format(item)
        else:
            return "[]"


class NumberSchemaDescription(SchemaDescription):

    def details_string(self):
        return "<number>"


class IntegerSchemaDescription(SchemaDescription):

    def details_string(self):
        return "<integer>"


class StringSchemaDescription(SchemaDescription):

    def details_string(self):
        return '"<string>"'


class EnumSchemaDescription(SchemaDescription):

    def __init__(self, schema, depth):
        super(EnumSchemaDescription, self).__init__(schema, depth)
        self.values = schema['enum']

    @staticmethod
    def value_string(v):
        if isinstance(v, str) or isinstance(v, unicode):
            return '"{}"'.format(v)
        else:
            return "{}".format(v)

    def details_string(self):

        if len(self.values) == 1:
            return self.value_string(self.values[0])
        else:
            return "{" + ', '.join(self.value_string(v) for v in self.values) + "}"


class MultiTypeSchemaDescription(SchemaDescription):

    def __init__(self, schema, depth):
        super(MultiTypeSchemaDescription, self).__init__(schema, depth)
        self.sub_schemas = [self.type_lookup()[t](schema, depth) for t in schema['type']]

    @staticmethod
    def value_string(v):
        if isinstance(v, str) or isinstance(v, unicode):
            return '"{}"'.format(v)
        else:
            return "{}".format(v)

    def details_string(self):

        if len(self.sub_schemas) == 1:
            return self.sub_schemas[0].details_string()
        else:
            return "{" + ', '.join(s.details_string() for s in self.sub_schemas) + "}"
