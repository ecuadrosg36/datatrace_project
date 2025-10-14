class GeneratorException(Exception):
    pass


class NonRegisteredStatementType(GeneratorException):
    pass


class MalformedInput(GeneratorException):
    pass
