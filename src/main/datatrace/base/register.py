from datatrace.base.factory import GeneratorFactory

from datatrace.base.statements.merge_statement import MergeStatement
from datatrace.base.statements.insert_into_select_statement import InsertIntoSelectStatement
from datatrace.base.statements.create_table_as_statement import CreateTableAsStatement
from datatrace.base.statements.spool_statement import SpoolStatement
from datatrace.base.statements.update_statement import UpdateStatement
from datatrace.base.statements.delete_statement import DeleteStatement
from datatrace.base.statements.begin_end_statement import BeginEndStatement


def register_statement_types():
    GeneratorFactory.register_template_type(MergeStatement, "MERGE_INTO")
    GeneratorFactory.register_template_type(BeginEndStatement, "BEGIN_END")
    GeneratorFactory.register_template_type(InsertIntoSelectStatement, "INSERT_INTO")
    GeneratorFactory.register_template_type(InsertIntoSelectStatement, "INSERT_INTO_SELECT")
    GeneratorFactory.register_template_type(CreateTableAsStatement, "CREATE_TABLE_AS")
    GeneratorFactory.register_template_type(SpoolStatement, "SPOOL_SELECT")
    GeneratorFactory.register_template_type(UpdateStatement, "UPDATE")
    GeneratorFactory.register_template_type(DeleteStatement, "DELETE")