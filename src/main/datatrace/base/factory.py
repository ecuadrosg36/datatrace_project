from abc import ABC, abstractmethod

from datatrace.base.statements.generator_statement import GeneratorStatement
from datatrace.base.statements.not_covered_statement import NotCoveredStatement

class GeneratorFactory(ABC):
    allowed_statement_types = {}

    def __init__(self):
        pass

    @classmethod
    def from_statement_type(cls, raw_statement: str, statement_type: str, statement_id: str) -> GeneratorStatement:
        if statement_type not in cls.allowed_statement_types:
            return NotCoveredStatement(raw_statement=raw_statement, statement_id=statement_id)

        obj = cls.allowed_statement_types[statement_type]
        return obj(raw_statement=raw_statement, statement_id = statement_id)

    @classmethod
    def register_template_type(cls, klass: GeneratorStatement, statement_type: str) -> None:
        cls.allowed_statement_types[statement_type] = klass