

class SchemaDescription(object):
    """
    Provide a description of a JSON schema for user consumption
    """

    @staticmethod
    def from_schema(schema, depth=0):

        if 'enum' in schema:
            return EnumSchemaDescription(schema, depth)

        return {
            "object": ObjectSchemaDescription,
            "array": ArraySchemaDescription,
            "number": NumberSchemaDescription,
            "string": StringSchemaDescription,
            "integer": IntegerSchemaDescription
        }[schema['type']](schema, depth)

    def __init__(self, schema, depth):
        self.description = schema.get("description", "(none)")
        self.title = schema.get("title", None)
        self.required = schema.get("required", False)
        self.depth = depth

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __unicode__(self):
        str = ""
        if self.title:
            str += "{}\n{}\n".format(self.title, ("-" * len(self.title)))
        str += "# {}\n".format(self.description)
        str += self.details_string()
        if self.title:
            str += "\n--------------------------------------------------"
        return str

    @property
    def newline(self):
        return '\n' + '    ' * self.depth

    def details_string(self):
        raise NotImplementedError


class ObjectSchemaDescription(SchemaDescription):

    def __init__(self, schema, depth):
        super(ObjectSchemaDescription, self).__init__(schema, depth)

        self.properties = { k: SchemaDescription.from_schema(v, depth+1) for k, v in schema['properties'].iteritems() }

    def details_string(self):
        if len(self.properties) == 0:
            properties = ""
        else:
            properties = self.newline
            properties += ',{0}{0}'.format(self.newline).join(
                '  # {}{}  {}: {}'.format(prop_schema.description, self.newline, prop, prop_schema.details_string())
                for prop, prop_schema in self.properties.iteritems())
            properties += self.newline

        return "{{{}}}".format(properties)


class ArraySchemaDescription(SchemaDescription):

    def details_string(self):
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
