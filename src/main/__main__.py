import sys
from datatrace.datatrace import DataTrace
from datatrace.utils.logger import log

def main():
    arguments = sys.argv
    if len(arguments) == 4:
        script_path = arguments[1]
        output_path = arguments[2]
        employee_code = arguments[3]

        dt = DataTrace()

        output_dict = dt.run_data_tracer(script_path=script_path, output_path=output_path, employee_code=employee_code)

    else:
        log.error("Se debe ingresar los argumentos de la ruta del script (o folder donde están los scripts)"
                  ", la ruta donde se guardará el excel de salida y la matrícula del Usuario")

if __name__ == '__main__':
    main()