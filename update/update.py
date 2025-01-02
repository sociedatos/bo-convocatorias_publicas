#!/usr/bin/env python3

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.ssl_ import create_urllib3_context
import urllib3
from bs4 import BeautifulSoup
import pandas as pd
import datetime as dt
import os
import argparse
import re
import sys

REINTENTOS = 20
TIMEOUT = 30
BASE_URL = "https://www.sicoes.gob.bo/portal"
SICOES_CIPHERS = "AES256-SHA"


def iniciarSesion(conexion: requests.sessions.Session, headers: dict, data: dict):
    """Una consulta para comenzar o reiniciar el intercambio con SICOES"""
    respuesta = conexion.get(
        BASE_URL + "/contrataciones/busqueda/convocatorias.php?tipo=convNacional",
        headers=headers,
        timeout=TIMEOUT,
        verify=False,
    )
    return mantenerSesion(data, respuesta)


def mantenerSesion(data: dict, response: requests.models.Response):
    """Actualiza los parámetros que mantienen una sesión"""
    parametros_sesion = ["B903A6B7", "varSesionCli"]

    html = BeautifulSoup(response.text, "html.parser")

    for parametro in parametros_sesion:
        valor = html.select(f"input[name={parametro}]")
        if len(valor) > 0:
            data[parametro] = valor[0]["value"]

    return data


def iniciarDescarga():
    """
    Pasos antes de descargar datos de convocatorias:
    - Crea una conexión para reutilizar en cada consulta.
    - Define un estado inicial para headers y datos de consulta.
    - Define el día de consulta desde el argumento --dia o , por defecto, el día de ayer.
    Retorna estos valores para futuras consultas y procesos.
    """

    class adaptadorSicoes(HTTPAdapter):
        """
        Un adaptador para realizar consultas con el cipher anticuado que utiliza SICOES.
        """

        def __init__(self, *args, **kwargs):
            self.ssl_context = create_urllib3_context(ciphers=SICOES_CIPHERS)
            self.ssl_context.check_hostname = False
            super().__init__(*args, **kwargs)

        def _add_ssl_context(self, *args, **kwargs):
            kwargs["ssl_context"] = self.ssl_context
            return super().init_poolmanager(*args, **kwargs)

        init_poolmanager = _add_ssl_context
        proxy_manager_for = _add_ssl_context

    def dataInicial():
        """
        Valores iniciales para datos de consulta.
        """

        return {
            "entidad": "",
            "codigoDpto": "",
            "cuce1": "",
            "cuce2": "",
            "cuce3": "",
            "cuce4": "",
            "cuce5": "",
            "cuce6": "",
            "objetoContrato": "",
            "codigoModalidad": "",
            "r1": "",
            "codigoContrato": "",
            "nroContrato": "",
            "codigoNormativa": "",
            "montoDesde": "",
            "montoHasta": "",
            "publicacionDesde": "",
            "publicacionHasta": "",
            "presentacionPropuestasDesde": "",
            "presentacionPropuestasHasta": "",
            "desiertaDesde": "",
            "desiertaHasta": "",
            "subasta": "",
            "personaContDespliegue": "on",
            "nomtoGarDespliegue": "option2",
            "costoPlieDespliegue": "option3",
            "arpcDespliegue": "option3",
            "fechaReunionDespliegue": "option1",
            "fechaAdjudicacionDespliegue": "option2",
            "dptoDespliegue": "option3",
            "normativaDespliegue": "option3",
            "tipo": "Avanzada",
            "operacion": "convNacional",
            "autocorrector": "",
            "nroRegistros": "10",
            "draw": "1",
            "start": "0",
            "length": "10",
            "captcha": "",
        }

    def headersIniciales():
        """Valores iniciales para headers de consulta."""
        return {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)",
            "Referer": "https://www.sicoes.gob.bo/portal/index.php",
        }

    def diaConsulta(data):
        """
        Define el día de consulta desde el argumento --dia o, por defecto, el día de ayer.
        """

        parametros_fecha = ["publicacionDesde", "publicacionHasta"]

        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--dia", required=False, type=str, help="Fecha de Publicación"
        )
        args = parser.parse_args()

        if args.dia:
            dia = dt.datetime.strptime(args.dia, "%Y-%m-%d")
        else:
            dia = dt.datetime.now() - dt.timedelta(days=1)

        for parametro in parametros_fecha:
            data[parametro] = dia.strftime("%d/%m/%Y")

        print(f"Convocatorias para el {dia.strftime("%Y-%m-%d")}")

        return dia, data

    # Utiliza el adaptador de SICOES en todas las consultas de red
    # y deshabilita advertencias para hacer consultas no cifradas.
    conexion = requests.Session()
    conexion.mount("http://", adaptadorSicoes(max_retries=REINTENTOS))
    conexion.mount("https://", adaptadorSicoes(max_retries=REINTENTOS))
    urllib3.disable_warnings()

    # Inicializa headers y datos de consulta.
    headers = headersIniciales()
    data = dataInicial()

    # Inicializa la sesión en SICOES.
    data = iniciarSesion(conexion, headers, data)

    # Define el día de consulta.
    dia, data = diaConsulta(data)

    # Retorna estos valores para futuras consultas y procesos.
    return conexion, headers, data, dia


def descargarConvocatorias(
    conexion: requests.sessions.Session, headers: dict, data: dict
):
    """
    Intenta descargar los registros de una página de convocatorias.
    Si la descarga es exitosa,
    - actualiza parámetros para mantener la sesión,
    - organiza las convocatorias en una lista de diccionarios
    - y las agrega a una lista final.
    Si una respuesta incluye menos de 10 convocatorias, ordena detener futuras descargas.
    Si una respuesta indica explícitamente "error", reinicia la sesión con SICOES y vuelve a intentar.
    Si una respuesta está vacía o malformada, indicando un error serio en cómo SICOES almacena y comunica
    estos registros, evita esa página y salta a la siguiente.
    Si una respuesta presenta otro tipo de error, aborta el programa para ser inspeccionado manualmente.
    """

    def leerCampo(campo: str, encoding: str = "iso-8859-1"):
        if campo and len(re.findall(r"(\%[0-9A-F][0-9A-F]*)", campo)) > 3:
            return bytes.fromhex(campo.replace("%", "")).decode(encoding)
        else:
            return campo

    def leerConvocatorias(respuesta_json: dict, encoding: str = "iso-8859-1"):
        return [
            {campo: leerCampo(item[campo], encoding) for campo in item.keys()}
            for item in respuesta_json["data"]
        ]

    detener = False
    try:
        respuesta = conexion.post(
            BASE_URL + "/contrataciones/operacion.php",
            headers=headers,
            data=data,
            timeout=TIMEOUT,
            verify=False,
        )
        respuesta_json = respuesta.json()
        if "error" in respuesta_json.keys():
            data = iniciarSesion(conexion, headers, data)
        else:
            data = mantenerSesion(data, respuesta)
            convocatorias_pagina = leerConvocatorias(respuesta_json)
            convocatorias.extend(convocatorias_pagina)
            print(f"{len(convocatorias)}/{respuesta_json["recordsTotal"]}")
            if len(convocatorias_pagina) < 10:
                detener = True
            else:
                data["draw"] = str(int(data["draw"]) + 1)
        return data, detener
    except requests.exceptions.JSONDecodeError:
        print(
            f"Error: una respuesta vacía o malformada. Saltando la página {data["draw"]}."
        )
        errores.append(data["draw"])
        if len(errores) >= 3:
            print("Demasiadas respuestas malformadas: el servidor está descompuesto. Deteniendo el programa.")
            sys.exit(1)
        else:
            data["draw"] = str(int(data["draw"]) + 1)
        return data, detener
    except Exception as e:
        print(f"Error: {e}. Deteniendo el programa.")
        sys.exit(1)


def estructurarConvocatorias(convocatorias: list):
    """Construye y sa forma a un dataframe con registros de convocatorias"""
    columnas = [
        "cuce",
        "entidad",
        "tipo_de_contratacion",
        "modalidad",
        "objeto_de_contratacion",
        "subasta",
        "fecha_publicacion",
        "fecha_presentacion",
        "estado",
        "archivos",
        "formularios",
        "ficha_del_proceso",
        "persona_contacto",
        "garantia",
        "costo_pliego",
        "arpc",
        "reunion_aclaracion",
        "fecha_adjudicacion_desierta",
        "departamento",
        "normativa",
    ]
    columnas_fecha = [
        "fecha_presentacion",
        "fecha_publicacion",
        "fecha_adjudicacion_desierta",
    ]

    tabla = pd.DataFrame(convocatorias)
    tabla.columns = columnas
    for columna in columnas_fecha:
        tabla[columna] = pd.to_datetime(
            tabla[columna], format="%d/%m/%Y", errors="coerce"
        )
    tabla["subasta"] = tabla.subasta.map({"Si": True, "No": False})
    tabla.drop(
        columns=[
            "garantia",
            "costo_pliego",
            "arpc",
            "reunion_aclaracion",
            "ficha_del_proceso",
            "archivos",
            "formularios",
        ],
        inplace=True,
    )
    return tabla


def actualizarRegistro(tabla: pd.core.frame.DataFrame, dia: dt.datetime):
    """
    Actualiza o crea el csv mensual donde se guardan los registros
    y el índice con el número de registros por mes
    """

    def actualizarIndice(tabla: pd.core.frame.DataFrame, dia: dt.datetime):
        indice_documento = "indice.csv"

        mes = dia.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        indice = pd.read_csv(indice_documento, parse_dates=["mes"])
        if mes in indice.mes.tolist():
            indice.loc[indice.mes == pd.to_datetime(mes), "convocatorias"] = (
                tabla.shape[0]
            )
        else:
            indice.loc[-1] = [mes, tabla.shape[0]]
        indice.to_csv(indice_documento, index=False, date_format="%Y-%m-%d")

    columnas_fecha = [
        "fecha_presentacion",
        "fecha_publicacion",
        "fecha_adjudicacion_desierta",
    ]
    documento_mes = f"data/{dia.strftime('%Y%m')}.csv"
    if os.path.exists(documento_mes):
        historico = pd.read_csv(documento_mes, parse_dates=columnas_fecha)
        tabla = pd.concat([historico, tabla])
    tabla.sort_values(["fecha_publicacion", "cuce"]).to_csv(documento_mes, index=False)
    actualizarIndice(tabla, dia)


if __name__ == "__main__":
    convocatorias, errores = [], []
    conexion, headers, data, dia = iniciarDescarga()
    while True:
        data, detener = descargarConvocatorias(conexion, headers, data)
        if detener:
            break
    if convocatorias:
        tabla = estructurarConvocatorias(convocatorias)
        actualizarRegistro(tabla, dia)
