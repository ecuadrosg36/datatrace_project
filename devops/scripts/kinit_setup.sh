#!/usr/bin/env bash

echo "Iniciando script para actualizar cache de Kerberos..."

if [[ $# -ne 2 ]]; then
    echo "Se esperaban 2 parametros y se recibieron $#:"
    echo "1. Archivo Keytab. Se recibio: ${1}" >&2
    echo "2. Principal. Se recibio: ${2}" >&2
    exit 64
fi

KEYTAB_NAME=$1
PRINCIPAL=$2
USER_NAME="$(whoami)"

export KRB5CCNAME="FILE:/tmp/krb5cc_jenkins_${USER_NAME}_${KEYTAB_NAME}"

sec_ahora="$(date +%s)"
sec_archivo="1"

if [[ -e ${KRB5CCNAME} ]]; then
    sec_archivo="$(stat -c %Y ${KRB5CCNAME})"
fi

if [ "$?" = "0" ]; then
    let diff="${sec_ahora}-${sec_archivo}"
else
    # Si hubo error, es porque archivo no existe o no es accesible
    # Se crea una diferencia grande para ejecutar kinit
    diff="9999"
fi

# Verificar si archivo en cache tiene antiguedad mayor a una hora
if [[ ${diff} -gt 3600 ]]; then
    /usr/bin/kinit -Vkt /devops/keytab/${KEYTAB_NAME}.keytab ${PRINCIPAL}
fi