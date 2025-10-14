from abc import ABC, abstractmethod

class GeneratorStatement(ABC):

    def __init__(self, raw_statement: str):
        self.raw_statement = raw_statement

    @abstractmethod
    def get_field_mapping(self):
        pass
