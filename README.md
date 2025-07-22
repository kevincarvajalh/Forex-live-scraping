import re
import requests
import unicodedata
import pandas as pd
import warnings
import concurrent.futures
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from deep_translator import GoogleTranslator

warnings.filterwarnings("ignore")

# --- Secciones ---
SECCION_PRINCIPAL = 'https://www.forexlive.com/'
SECCION_CENTRAL_BANKS = 'https://www.forexlive.com/CentralBanks'

# --- Filtros ---
FILTROS_TITULARES = [
    'technical analysis','PBOC','MANUFACTURING','richmond','analisis tecnico', 'niveles', 'levels', 'pmi', 'flash pmi',
    'eurostoxx', 'calendar', 'usdchf', 'usdjpy', 'usdcad', 'fx option', 'kiwi',
    'open levels', 'fx news wrap', 'economic calendar', 'market outlook',
    'european recap', 'indicative prices', 'week ahead', 'daily preview',
    'option expiry', 'resistance', 'support', 'wrap', 'preview', 'forecast',
    's&p 500 analysis', 'momentum', 'fibonacci'
]

FRASES_ESPECULATIVAS = [
    'yet to come', 'comments tomorrow', 'awaiting statement', 'expected intervention',
    'possible speech', 'rumors about', 'expected to speak', 'imminent statement',
    'unconfirmed agenda', 'possible remarks', 'potential intervention',
    'reaction anticipated', 'markets await', 'waiting for comments',
    'no official confirmation', 'preliminary information'
]

EXPRESIONES_VACIAS = [
    "what's next", "the worst", "already happened", "markets needed", "dust settles",
    "everyone is watching", "waiting for", "a moment to watch", "we’ll know soon",
    "probably", "could be", "seems like", "it is said that", "it is speculated that"
]

# --- Oficiales por divisa ---
MIEMBROS = {
    'USD': ['BARR','BOSTIC','BOWMAN','COLLINS','COOK','DALY','GOOLSBEE','HAMMACK','JEFFERSON',
            'KASHKARI','KUGLER','LOGAN','MESTER','POWELL','WALLER','WILLIAMS'],
    'EUR': ['CENTENO','CIPOLLONE','DE GUINDOS','ELDERSON','HERODOTOU','HOLZMANN',
            'HERNANDEZ DE COS','KAZĀKS','KAŽIMÍR','KAZIMIR','KNOT','LAGARDE','LANE','MAECHLER',
            'MAKHLOUF','NAGEL','PANETTA','REHN','SCHNABEL','STOURNARAS','VASLE','VISCO',
            'VUJCIC','WUNSCH','ŠIMKUS'],
    'GBP': ['BAILEY','BREEDEN','BROADBENT','DHINGRA','GREENE','HASKEL','MANN','PILL','RAMSDEN'],
    'AUD': ['BARNABA','BULLOCK','HAUSER','HUNTER','JONES','RUBIN','WATKINS','WILKINS'],
    'NZD': ['CONWAY','HARRIS','ORR','QUIGLEY','SILK'],
    'JPY': ['ADACHI','HIMINO','NAKAMURA','NOGUCHI','TAKATA','TAMURA','UEDA','UCHIDA'],
    'CAD': ['BEAUDRY','KOZICKI','MACKLEM','MENDES','ROGERS','VINCENT']
}

# --- Diccionario por divisa con claves combinadas ---
CLAVES_DIVISAS = [
    ('USD', ['FED','FOMC','TREASURY','DOLLAR','EE. UU.','ESTADOS UNIDOS','YIELDS','US10Y'] + MIEMBROS['USD']),
    ('EUR', ['ECB','BCE','FRANCE','eur','euro','spanish','ITALY','GERMANY','EUROZONE','EUROPA','EUROSTAT','IFO','ZEW'] + MIEMBROS['EUR']),
    ('GBP', ['BOE','BANK OF ENGLAND','POUND','STERLING','REINO UNIDO','UK','FTSE','SUNAK'] + MIEMBROS['GBP']),
    ('JPY', ['BOJ','JAPAN','YEN','TOKYO','NIKKEI','KISHIDA'] + MIEMBROS['JPY']),
    ('AUD', ['RBA','AUSTRALIA','AUSSIE','SYDNEY'] + MIEMBROS['AUD']),
    ('CAD', ['BOC','BANK OF CANADA','CANADA','LOONIE','OTTAWA'] + MIEMBROS['CAD']),
    ('NZD', ['RBNZ','RESERVE BANK OF NEW ZEALAND','KIWI','WELLINGTON'] + MIEMBROS['NZD']),
    ('CHF', ['SNB','SWITZERLAND','FRANC','ZURICH','JORDAN','MAECHLER'])
]

# --- Palabras clave adicionales (rellena si las usas en tu análisis) ---
INSTITUCIONES_RELEVANTES = [
    # Bancos de inversión y análisis
    'ALLIANZ','MUFG:','ANZ','journal','wsj report', 'BANK', 'BANK OF AMERICA', 'BARCLAYS', 'BLACKROCK', 'BOFA',
    'BNP PARIBAS', 'CAPITAL ECONOMICS', 'CITI', 'CITIGROUP', 'CREDIT SUISSE',
    'DEUTSCHE BANK', 'EVERCORE', 'GOLDMAN', 'GOLDMAN SACHS', 'HSBC GLOBAL RESEARCH',
    'ING', 'JANUS HENDERSON', 'JEFFERIES', 'J.P. MORGAN', 'MACQUARIE',
    'MARKETWATCH', 'MORGAN STANLEY', 'OCDE', 'OXFORD ECONOMICS',
    'PANTHEON MACROECONOMICS', 'PIMCO', 'RBC', 'REUTERS', 'SCOTIABANK',
    'SCHRODERS', 'SOCIETE GENERALE', 'STANDARD CHARTERED', 'TD SECURITIES',
    'THE ECONOMIST', 'TS LOMBARD', 'UBS', 'VANGUARD', 'WALL STREET JOURNAL',
    'WELLS FARGO', 'WESTPAC', 'WORLD BANK', 'WSJ', 'YAHOO FINANCE',
    'BIS', 'IMF', 'FT', 'FINANCIAL TIMES', 'CNBC', 'BLOOMBERG', 'NIKKEI ASIA', "BARRON'S"
]

PERSONAJES_GEOPOLITICOS = ['DONALD TRUMP','trump','nuclear','israel','yemen','iran','putin','rusia',
    'AMIR-ABDOLLAHIAN', 'BAERBOCK', 'BIDEN', 'BLINKEN', 'CAMERON', 'CHARLES MICHEL',
    'CLAUDIA SHEINBAUM', 'ERDOGAN', 'FIDAN', 'GALLANT', 'HADDAD', 'HERZOG', 'JAISHANKAR',
    'JAVIER MILEI', 'KHAMENEI', 'KIM JONG UN', 'KULEBA', 'LAVROV', 'LE MAIRE', 'LI QIANG',
    'LOPEZ OBRADOR', 'LULA', 'MACRON', 'MEDVEDEV', 'MODI', 'NETANYAHU', 'PUTIN', 'RAISI',
    'SCHOLZ', 'STARER', 'TRUMP', 'URSULA VON DER LEYEN', 'WANG YI', 'XI JINPING',
    'YELLEN', 'ZELENSKY'
]

PALABRAS_GEOPOLITICAS = ['iran','israel','yemen','trump',
    'agriculture ban', 'alliance','CRUDE', 'aluminum', 'army', 'attack', 'barrel', 'blockade',
    'bombing', 'bombs', 'border', 'clash', 'commodity prices', 'conflict',
    'container rates', 'copper', 'crude', 'defense', 'demilitarized zone',
    'deployment', 'deterrence', 'diesel', 'diplomacy', 'embargo', 'energy crisis',
    'energy embargo', 'energy prices', 'escalation', 'fertilizer', 'fighter jets',
    'food security', 'freight', 'fuel', 'gas exports', 'gasoline', 'gaza', 'gold',
    'grain exports', 'hedging', 'hormuz', 'inflation', 'international crisis',
    'iron ore', 'israel', 'lithium', 'lng', 'market volatility', 'maritime security',
    'military', 'military alliance', 'missiles', 'natural gas', 'naval blockade',
    'nato', 'nickel', 'nuclear', 'occupation', 'opec', 'opec+', 'panama canal',
    'pentagon', 'pipeline', 'platinum', 'price shock', 'rare earths', 'red sea',
    'refinery', 'retaliation', 'sanctions', 'shipping lanes', 'silver',
    'south china sea', 'spr', 'steel', 'strategic reserves', 'strategic stockpile',
    'strait of hormuz', 'suez canal', 'summit', 'supply chain', 'supply disruption',
    'tanker', 'terrorism', 'troops', 'uranium', 'war', 'weapons'
]

# --- Utilidades ---
def normalizar(texto): return unicodedata.normalize('NFD', texto).encode('ascii', 'ignore').decode().lower()
def contiene_palabra_clave(texto, lista): return any(normalizar(p) in normalizar(texto) for p in lista)
def tiene_minimo_de_palabras_clave(texto, lista, minimo=2):
    return sum(1 for palabra in lista if normalizar(palabra) in normalizar(texto)) >= minimo

def limpiar_traduccion(texto): return texto.replace("de guerra de guerra", "geopolitica").replace("ICYMI", "").strip().capitalize()
cache_traducciones = {}
def traducir_titulo(titulo):
    if titulo in cache_traducciones: return cache_traducciones[titulo]
    try:
        traduccion = GoogleTranslator(source='auto', target='es').translate(titulo)
        cache_traducciones[titulo] = limpiar_traduccion(traduccion)
        return cache_traducciones[titulo]
    except: return titulo

def detectar_divisa(titulo):
    texto_n = normalizar(titulo)
    for divisa, claves in CLAVES_DIVISAS:
        if sum(1 for clave in claves if normalizar(clave) in texto_n) >= 1:
            return divisa
    return 'OTRO'

def detectar_divisa_por_nombre(titulo):
    texto_n = normalizar(titulo)
    for divisa, nombres in MIEMBROS.items():
        if any(normalizar(nombre) in texto_n for nombre in nombres):
            return divisa
    return None

def extraer_fecha_publicacion(soup):
    div = soup.find('div', class_='publisher-details__date')
    if div:
        m = re.search(r'(\d{2}/\d{2}/\d{4})\s*\|\s*(\d{2}:\d{2})', div.get_text())
        if m:
            try:
                dt = datetime.strptime(f"{m.group(1)} {m.group(2)}", "%d/%m/%Y %H:%M")
                return dt.strftime("%d-%m-%Y %H:%M")
            except:
                return 'SIN_FECHA'
    return 'SIN_FECHA'

def extraer_articulo(url):
    r = session.get(url, timeout=10)
    soup = BeautifulSoup(r.text, 'html.parser')
    art = soup.find('article', class_='article__content-body article-body')
    if not art: return None
    titulo_tag = soup.find('h1', class_='article__title')
    titulo = titulo_tag.text.strip() if titulo_tag else ''
    titulo = re.sub(r"\b([A-Za-z]{2,6})['’]s\b", r"\1", titulo)
    texto_filtrado = normalizar(titulo)

    if any(p in texto_filtrado for p in map(normalizar, FILTROS_TITULARES + FRASES_ESPECULATIVAS + EXPRESIONES_VACIAS)):
        return None

    divisa_funcionario = detectar_divisa_por_nombre(titulo)
    if divisa_funcionario:
        categoria = 'Declaracion'
        divisa = divisa_funcionario
    elif contiene_palabra_clave(titulo.upper(), [i.upper() for i in INSTITUCIONES_RELEVANTES]) and any(contiene_palabra_clave(titulo.upper(), [kw.upper() for kw in claves]) for _, claves in CLAVES_DIVISAS):
        categoria = 'Extra'
        divisa = detectar_divisa(titulo)
    elif tiene_minimo_de_palabras_clave(titulo, PERSONAJES_GEOPOLITICOS) or tiene_minimo_de_palabras_clave(titulo, PALABRAS_GEOPOLITICAS):
        categoria = 'Mundo'
        divisa = 'Geopolitica'
    else:
        return None

    fecha = extraer_fecha_publicacion(soup)
    if fecha == 'SIN_FECHA':
        return None

    return {
        'FECHA-HORA': fecha,
        'DIVISA': divisa,
        'CATEGORIA': categoria,
        'TITULAR (ES)': limpiar_traduccion(traducir_titulo(titulo))
    }

# --- Wrapper con control de errores ---
def extraer_articulo_wrapper(url):
    try:
        return extraer_articulo(url)
    except Exception as e:
        print(f"⚠️ Error al procesar {url} — {e}")
        return None

# --- Sesión y scraping ---
session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0'})

secciones = [
    "https://www.forexlive.com/",
    "https://www.forexlive.com/centralbank/"
]

noticias = []
stop_scraping = False
hoy = datetime.now()
inicio_semana = hoy - timedelta(days=hoy.weekday() + 1)

for seccion in secciones:
    for i in range(1, 30):
        if stop_scraping:
            break
        r = session.get(f"{seccion}page/{i}/", timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')
        links = soup.select("h3.article-slot__title.title.top a")
        urls = [f"https://www.forexlive.com{a.get('href')}" for a in links if '/Education/' not in a.get('href')]

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(extraer_articulo_wrapper, urls))

        for nota in results:
            if nota:
                try:
                    fecha_dt = datetime.strptime(nota['FECHA-HORA'], "%d-%m-%Y %H:%M")
                except Exception as e:
                    print(f"⚠️ Fecha inválida: {nota['FECHA-HORA']} — {e}")
                    continue
                if fecha_dt >= inicio_semana or fecha_dt.weekday() == 6:
                    noticias.append(nota)
                else:
                    stop_scraping = True
                    break

# --- Exportar resultados ---
if noticias:
    df = pd.DataFrame(noticias).drop_duplicates(subset=['TITULAR (ES)'])
    df = df[df['DIVISA'] != 'OTRO']
    df = df.sort_values(by='FECHA-HORA')
    df.to_excel("forexlive_eventos_relevantes.xlsx", index=False, engine='openpyxl')
    print(f"✅ {len(df)} artículos guardados en 'forexlive_eventos_relevantes.xlsx'")
else:
    print("⚠️ No se encontraron eventos relevantes.")
