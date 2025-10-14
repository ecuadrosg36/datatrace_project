from unittest import TestCase
from main.datatrace.utils.logger import log
from main.datatrace.base.statements.select_statement import SelectStatement

class Test(TestCase):

    def test_intepret(self):
        map_answers = {"""Select CODLOTEOFERTA, PLASTICO,
                            MEDIAN(MTOINGRESOCALCULADO) MTOINGRESOCALCULADO_MED
                            From TEMP_INPUT_CLV_TC_VARP_CODMES._ORIG_22_VENTA_EP_BT
                            Where MTOINGRESOCALCULADO Is Not Null
                            GROUP BY CODLOTEOFERTA, PLASTICO""": 5}
        for query_str, answer in map_answers.items():
            select_query = SelectStatement(raw_statement=query_str)

        pass