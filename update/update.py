#!/usr/bin/env python3

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.ssl_ import create_urllib3_context
from bs4 import BeautifulSoup
import pandas as pd
import datetime as dt
import os
import argparse

MAX_RETRIES = 20
TIMEOUT = 30
SICOES_CIPHERS = ('AES256-SHA')
BASE_URL = 'https://www.sicoes.gob.bo/portal'

headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)',
    'Referer': 'https://www.sicoes.gob.bo/portal/index.php'
}

data = {
    'entidad': '',
    'codigoDpto': '',
    'cuce1': '',
    'cuce2': '',
    'cuce3': '',
    'cuce4': '',
    'cuce5': '',
    'cuce6': '',
    'objetoContrato': '',
    'codigoModalidad': '',
    'r1': '',
    'codigoContrato': '',
    'nroContrato': '',
    'codigoNormativa': '',
    'montoDesde': '',
    'montoHasta': '',
    'publicacionDesde': '',
    'publicacionHasta': '',
    'presentacionPropuestasDesde': '',
    'presentacionPropuestasHasta': '',
    'desiertaDesde': '',
    'desiertaHasta': '',
    'subasta': '',
    'personaContDespliegue': 'on',
    'nomtoGarDespliegue': 'option2',
    'costoPlieDespliegue': 'option3',
    'arpcDespliegue': 'option3',
    'fechaReunionDespliegue': 'option1',
    'fechaAdjudicacionDespliegue': 'option2',
    'dptoDespliegue': 'option3',
    'normativaDespliegue': 'option3',
    'tipo': 'Avanzada',
    'operacion': 'convNacional',
    'autocorrector': '',
    'nroRegistros': '10',
    'draw': '1',
    'start': '0',
    'length': '10',
    'captcha': '',
}

DATA_COLUMNS = [
	'cuce',
	'entidad',
	'tipo_de_contratacion',
	'modalidad',
	'objeto_de_contratacion',
	'estado',
	'subasta',
	'fecha_presentacion',
	'fecha_publicacion',
	'archivos',
	'formularios',
	'ficha_del_proceso',
	'persona_contacto',
	'garantia',
	'costo_pliego',
	'arpc',
	'reunion_aclaracion',
	'fecha_adjudicacion_desierta',
	'departamento',
	'normativa'
]
DATE_COLUMNS = [
    'fecha_presentacion',
    'fecha_publicacion',
    'fecha_adjudicacion_desierta'
]

class sicoesAdapter(HTTPAdapter):

    def init_poolmanager(self, *args, **kwargs):
        context = create_urllib3_context(ciphers=SICOES_CIPHERS)
        kwargs['ssl_context'] = context
        return super(sicoesAdapter, self).init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        context = create_urllib3_context(ciphers=SICOES_CIPHERS)
        kwargs['ssl_context'] = context
        return super(sicoesAdapter, self).proxy_manager_for(*args, **kwargs)

def requests_session() -> requests.sessions.Session:
    session = requests.Session()
    session.mount("http://", sicoesAdapter(max_retries=MAX_RETRIES))
    session.mount("https://", sicoesAdapter(max_retries=MAX_RETRIES))
    return session

def what_day():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dia', required=False, type=str, help='Fecha de Publicación')
    args = parser.parse_args()
    if args.dia:
        return dt.datetime.strptime(args.dia, '%Y-%m-%d')
    else:
        return (dt.datetime.now() - dt.timedelta(days=1))


def update_data(response):
    html = BeautifulSoup(response.text, 'html.parser')
    for name in ['B903A6B7', 'varSesionCli']:
        i = html.select('input[name={}]'.format(name))
        if len(i) > 0:
            data[name] = i[0]['value']

def get_session():
    global data
    global cookies

    sess = requests_session()

    url = BASE_URL + '/index.php'
    response = sess.get(url, headers=headers, timeout=TIMEOUT)

    url = BASE_URL + '/contrataciones/busqueda/convocatorias.php?tipo=convNacional'
    response = sess.get(url, headers=headers, timeout=TIMEOUT)
    update_data(response)

    return sess


def parse_field(field, encoding):
    if '%' in field:
        return bytes.fromhex(field.replace('%', '')).decode(encoding)
    else:
        return field


def parse_results(response_json, encoding='iso-8859-1'):
    return [{field: parse_field(item[field], encoding) for field in item.keys()} for item in response_json['data']]


def search(sess):
    global total_results

    url = BASE_URL + '/contrataciones/operacion.php'
    response = sess.post(url, headers=headers, data=data, timeout=TIMEOUT)

    if 'error' not in response.json().keys():
        update_data(response)
        r = response.json()
        total_results = r['recordsTotal']
        return parse_results(r)

    else:
        return 'error'


def search_all(sess):
    while True:
        results = search(sess)
        if results == 'error':
            sess = get_session()

        else:
            print('{}/{}'.format((int(data['draw']) - 1) * 10, total_results))
            all_results.extend(results)
            data['draw'] = str(int(data['draw']) + 1)

            if len(results) < 10:
                break

def format_results(results):
    df = pd.DataFrame(results)
    df.columns = DATA_COLUMNS

    for col in DATE_COLUMNS:
        df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')

    df['subasta'] = df['subasta'].map({'Si': True, 'No': False})

    df.drop(
        columns=[
            'garantia', 'costo_pliego', 'arpc', 'reunion_aclaracion',
            'ficha_del_proceso', 'archivos', 'formularios'
        ],
        inplace=True
    )

    return df


def update_indice(dia, df):
    este_mes = dia.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    indice = pd.read_csv('indice.csv', parse_dates=['mes'])
    if (indice.mes == este_mes.strftime('%Y-%m-%d')).sum() > 0:
        indice.loc[indice.mes == este_mes.strftime('%Y-%m-%d'), 'convocatorias'] = df.shape[0]
    else:
        indice.loc[-1] = [este_mes, df.shape[0]]
    indice.to_csv('indice.csv', index=False, date_format='%Y-%m-%d')

if __name__ == '__main__':
    sess = get_session()

    # ayer = (dt.datetime.now() - dt.timedelta(days=1))
    dia = what_day()
    for fecha in ['publicacionDesde', 'publicacionHasta']:
        data[fecha] = dia.strftime('%d/%m/%Y')
    all_results = []
    total_results = 0

    search_all(sess)
    if len(all_results) > 0:
        df = format_results(all_results)

        filename = 'data/{}.csv'.format(dia.strftime('%Y%m'))
        if os.path.exists(filename):
            old = pd.read_csv(filename, parse_dates=DATE_COLUMNS)
            df = pd.concat([old, df])

        df.sort_values([
            'fecha_publicacion', 'cuce'
        ]).to_csv(filename, index=False)

        update_indice(dia, df)