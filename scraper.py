# -*- coding: utf-8 -*-
"""
Scraper + actualización de Google Sheets (P-web y Jumbo) vía API (gspread).
Versión mejorada con estrategias de extracción más robustas.

Lee hoja 'Jumbo-info' (SKU=B, URL=D, Peso Jumbo=E, Precio GM=F, Peso GM=G).
Scrapea precio en la página de Jumbo con Selenium (Chrome headless).
Calcula "Precio por 1 kg Jumbo" = precio_scrapeado / PesoJumbo_g * 1000 (redondeado).

Actualiza:
  * Hoja 'P-web' -> Columna I ("Jumbo Kg"), por SKU:
      - Si hay valor nuevo, lo escribe.
      - Si NO hay valor nuevo (None), NO pisa el valor anterior.
  * Hoja 'Jumbo' (histórico) -> agrega columna con fecha dd-mm-YYYY, por SKU:
      - Si falta el SKU, lo agrega al final (col B = SKU).
      - Si el valor es None, deja celda vacía.

Variables de entorno requeridas:
- GCP_SHEETS_CREDENTIALS  (contenido JSON de Service Account)
- SHEET_ID                (ID del spreadsheet)
- CHROME_BIN              (opcional; si viene, lo usamos como binario de Chrome)
- DEBUG_MODE              (opcional; si es "true", guarda HTML de páginas fallidas)

Ejecución:
    python improved-scraper.py
"""

from __future__ import annotations
import os
import re
import time
import json
import random
import uuid
import tempfile
import hashlib
from typing import Optional, Tuple, List, Dict, Any
from datetime import datetime
from dateutil import tz
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# =========================
# Configuración de hojas / columnas
# =========================

SHEET_ID = os.getenv("SHEET_ID", "").strip()
if not SHEET_ID:
    raise RuntimeError("Falta SHEET_ID en variables de entorno.")

SHEET_JUMBO_INFO = "Jumbo-info"
SHEET_PWEB = "P-web"
SHEET_JUMBO_HIST = "Jumbo"

# Jumbo-info: B=SKU, D=URL, E=Peso Jumbo (g), F=Precio GM, G=Peso GM
COL_SKU_INFO = 2
COL_URL_INFO = 4
COL_PESO_JUMBO_INFO = 5
COL_PRECIO_GM_INFO = 6
COL_PESO_GM_INFO = 7

# P-web: B=SKU, I="Jumbo Kg"
COL_SKU_PWEB = 2
COL_JUMBO_KG_PWEB = 9  # Columna I

# Jumbo (histórico): B=SKU, columnas de fechas a partir de C
COL_SKU_HIST = 2
COL_FECHAS_INICIA_EN = 3  # Columna C

SLEEP_MIN = 0.8
SLEEP_MAX = 1.5

# Directorio para guardar HTML de páginas fallidas (debugging)
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
DEBUG_DIR = Path("debug_html")
if DEBUG_MODE:
    DEBUG_DIR.mkdir(exist_ok=True)

# =========================
# Autenticación Google
# =========================

def _get_gspread_client():
    creds_json = os.getenv("GCP_SHEETS_CREDENTIALS", "")
    if not creds_json:
        raise RuntimeError("Falta GCP_SHEETS_CREDENTIALS en variables de entorno (pegar JSON completo).")

    info = json.loads(creds_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    credentials = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(credentials)

def open_sheet():
    gc = _get_gspread_client()
    return gc.open_by_key(SHEET_ID)

# =========================
# Utilidades de precio mejoradas
# =========================

# Regex más flexibles para capturar diferentes formatos de precio
PRECIO_REGEXES = [
    # Formato estándar: $ 7.990 o $7.990
    re.compile(r"\$\s*([\d]{1,3}(?:\.[\d]{3})*)", re.IGNORECASE),
    # Formato con separador de miles usando coma: $ 7,990
    re.compile(r"\$\s*([\d]{1,3}(?:,[\d]{3})*)", re.IGNORECASE),
    # Formato CLP: CLP 7.990
    re.compile(r"CLP\s*([\d]{1,3}(?:\.[\d]{3})*)", re.IGNORECASE),
    # Formato sin símbolo pero con contexto de precio
    re.compile(r"(?:precio|price|valor|costo).*?([\d]{1,3}(?:\.[\d]{3})+)", re.IGNORECASE),
    # Números grandes que podrían ser precios (4+ dígitos)
    re.compile(r"(?<!\d)([\d]{1,3}(?:\.[\d]{3})+)(?!\d)")
]

# Palabras que indican que NO es el precio principal
PALABRAS_EXCLUIR = {
    "prime", "paga", "antes", "suscríbete", "suscribete", 
    "normal", "referencia", "comparar", "ahorra", "descuento",
    "cuotas", "envío", "envio", "despacho", "retiro"
}

# Palabras que indican que SÍ es probablemente el precio principal
PALABRAS_PRECIO_PRINCIPAL = {
    "precio", "valor", "ahora", "oferta", "promoción", 
    "promocion", "final", "total", "pagar"
}

def normaliza(texto: str) -> str:
    """Normaliza espacios en blanco en el texto."""
    return " ".join(str(texto).split()).strip()

def extraer_todos_los_precios(texto: str) -> List[Tuple[int, str]]:
    """
    Extrae TODOS los posibles precios del texto.
    Retorna lista de tuplas (precio, contexto) ordenada por probabilidad.
    """
    precios_encontrados = []
    lineas = texto.split('\n')
    
    for i, linea in enumerate(lineas):
        linea_norm = normaliza(linea)
        if not linea_norm:
            continue
            
        # Obtener contexto (línea anterior y siguiente)
        contexto = []
        if i > 0:
            contexto.append(normaliza(lineas[i-1]))
        contexto.append(linea_norm)
        if i < len(lineas) - 1:
            contexto.append(normaliza(lineas[i+1]))
        contexto_str = " ".join(contexto).lower()
        
        for rx in PRECIO_REGEXES:
            matches = rx.finditer(linea_norm)
            for m in matches:
                bruto = m.group(1).replace(".", "").replace(",", "")
                if bruto.isdigit():
                    precio = int(bruto)
                    # Filtrar precios poco probables
                    if 100 <= precio <= 9999999:  # Entre $100 y $9,999,999
                        score = calcular_score_precio(precio, contexto_str)
                        precios_encontrados.append((precio, contexto_str, score))
    
    # Ordenar por score (mayor score = más probable de ser el precio principal)
    precios_encontrados.sort(key=lambda x: x[2], reverse=True)
    
    # Retornar solo precio y contexto
    return [(p, c) for p, c, _ in precios_encontrados]

def calcular_score_precio(precio: int, contexto: str) -> float:
    """
    Calcula un score de probabilidad de que sea el precio principal.
    Mayor score = más probable.
    """
    score = 1.0
    
    # Penalizar si hay palabras de exclusión
    for palabra in PALABRAS_EXCLUIR:
        if palabra in contexto:
            score *= 0.3
    
    # Bonificar si hay palabras de precio principal
    for palabra in PALABRAS_PRECIO_PRINCIPAL:
        if palabra in contexto:
            score *= 1.5
    
    # Bonificar precios en rangos típicos
    if 500 <= precio <= 50000:  # Rango típico de productos de supermercado
        score *= 1.2
    
    # Penalizar precios muy pequeños o muy grandes
    if precio < 100 or precio > 1000000:
        score *= 0.5
    
    # Bonificar si el contexto contiene "agregar" o "comprar"
    if any(word in contexto for word in ["agregar", "añadir", "comprar", "carro", "bolsa"]):
        score *= 1.3
    
    return score

def es_precio_valido(txt: str) -> bool:
    """Determina si un texto podría contener un precio válido."""
    t = txt.lower()
    
    # Debe tener algún indicador de precio
    if not any(indicator in t for indicator in ["$", "clp", "precio", "valor", "."]):
        return False
    
    # Rechazar si tiene demasiadas palabras de exclusión
    exclusion_count = sum(1 for p in PALABRAS_EXCLUIR if p in t)
    if exclusion_count >= 2:
        return False
    
    return True

def precio_por_kg(precio: Optional[int], peso_gr: Optional[float]) -> Optional[int]:
    """Calcula el precio por kilogramo."""
    if precio is None or peso_gr is None:
        return None
    try:
        peso_gr = float(peso_gr)
        if peso_gr <= 0:
            return None
        return round(int(precio) / peso_gr * 1000)
    except Exception:
        return None

# =========================
# Selenium mejorado con estrategias múltiples
# =========================

def build_browser():
    """
    Chrome headless para CI con configuración optimizada.
    """
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-features=TranslateUI")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    profile_dir = os.path.join(tempfile.gettempdir(), f"chrome-profile-{uuid.uuid4()}")
    os.makedirs(profile_dir, exist_ok=True)
    options.add_argument(f"--user-data-dir={profile_dir}")
    options.add_argument("--remote-debugging-port=9222")

    chrome_bin = os.environ.get("CHROME_BIN", "").strip()
    if chrome_bin and os.path.exists(chrome_bin):
        options.binary_location = chrome_bin

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(90)
    driver.implicitly_wait(3)  # Espera implícita para elementos
    return driver

def esperar_carga_completa(driver: webdriver.Chrome, timeout: int = 15):
    """
    Espera a que la página esté completamente cargada, incluyendo AJAX.
    """
    try:
        # Esperar a que el DOM esté listo
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        
        # Esperar un poco más para contenido AJAX
        time.sleep(2)
        
        # Hacer scroll para activar lazy loading
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/3);")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
        time.sleep(1)
        
    except Exception as e:
        print(f"Advertencia en espera de carga: {e}")

def encontrar_contenedor_producto(driver: webdriver.Chrome) -> Optional[webdriver.remote.webelement.WebElement]:
    """
    Intenta encontrar el contenedor principal del producto.
    """
    selectores_contenedor = [
        "[class*='product-detail']",
        "[class*='product-info']",
        "[class*='product-container']",
        "[class*='detail-container']",
        "[data-testid*='product']",
        "main [class*='product']",
        "#product-detail",
        ".product-page",
        "[itemtype*='Product']",
        "[itemscope][itemtype*='schema.org']"
    ]
    
    for selector in selectores_contenedor:
        try:
            elementos = driver.find_elements(By.CSS_SELECTOR, selector)
            if elementos:
                # Retornar el elemento más grande (probablemente el contenedor principal)
                return max(elementos, key=lambda e: e.size.get('height', 0) * e.size.get('width', 0))
        except Exception:
            continue
    
    # Si no encuentra contenedor específico, usar body
    try:
        return driver.find_element(By.TAG_NAME, "body")
    except Exception:
        return None

def estrategia_selectores_mejorada(driver: webdriver.Chrome) -> Optional[int]:
    """
    Búsqueda mejorada usando selectores CSS con validación contextual.
    """
    # Selectores organizados por prioridad
    grupos_selectores = [
        # Alta prioridad: selectores específicos de precio
        [
            "[class*='price-now']",
            "[class*='sale-price']",
            "[class*='offer-price']",
            "[class*='current-price']",
            "[class*='final-price']",
            "[data-testid*='price']",
            "[data-qa*='price']",
            ".price-wrapper .price",
            ".product-price:not([class*='old']):not([class*='before'])",
        ],
        # Media prioridad: selectores genéricos de precio
        [
            ".price:not([class*='old']):not([class*='strike'])",
            "span[class*='price']:not([class*='old'])",
            "div[class*='price']:not([class*='old'])",
            "p[class*='price']:not([class*='old'])",
            "[class*='valor']",
            "[class*='precio']",
        ],
        # Baja prioridad: elementos genéricos con formato de moneda
        [
            "span:contains('$')",
            "div:contains('$')",
            "p:contains('$')",
            "strong:contains('$')",
            "b:contains('$')",
        ]
    ]
    
    precios_candidatos = []
    
    for grupo in grupos_selectores:
        for selector in grupo:
            try:
                # Usar XPath para selectores con :contains
                if ':contains' in selector:
                    base_selector = selector.split(':contains')[0]
                    elementos = driver.find_elements(By.CSS_SELECTOR, base_selector)
                else:
                    elementos = driver.find_elements(By.CSS_SELECTOR, selector)
                
                for elemento in elementos[:5]:  # Limitar a los primeros 5 de cada selector
                    if not elemento.is_displayed():
                        continue
                    
                    texto = normaliza(elemento.text)
                    if not texto:
                        continue
                    
                    # Obtener todos los precios del elemento
                    precios = extraer_todos_los_precios(texto)
                    
                    for precio, contexto in precios:
                        if precio:
                            # Calcular posición en la página (más arriba = mejor)
                            try:
                                location = elemento.location
                                y_position = location.get('y', 10000)
                                score = 10000 - y_position  # Invertir para que más arriba = mayor score
                            except Exception:
                                score = 0
                            
                            precios_candidatos.append((precio, score, contexto))
            except Exception:
                continue
        
        # Si encontramos precios en alta prioridad, no buscar en baja prioridad
        if precios_candidatos and grupo == grupos_selectores[0]:
            break
    
    if precios_candidatos:
        # Ordenar por score y retornar el mejor
        precios_candidatos.sort(key=lambda x: x[1], reverse=True)
        return precios_candidatos[0][0]
    
    return None

def estrategia_texto_completo(driver: webdriver.Chrome) -> Optional[int]:
    """
    Extrae todo el texto visible de la página y busca precios con regex.
    """
    try:
        # Obtener todo el texto visible
        body = driver.find_element(By.TAG_NAME, "body")
        texto_completo = body.text
        
        # Buscar todos los precios
        precios = extraer_todos_los_precios(texto_completo)
        
        if precios:
            # Filtrar precios poco probables
            precios_filtrados = []
            for precio, contexto in precios:
                # Verificar que no sea un SKU, código postal, año, etc.
                contexto_lower = contexto.lower()
                if not any(word in contexto_lower for word in 
                          ["código", "codigo", "sku", "postal", "año", "referencia", "id", "item"]):
                    precios_filtrados.append(precio)
            
            if precios_filtrados:
                # Si hay múltiples precios, preferir el que esté en un rango típico
                precios_tipicos = [p for p in precios_filtrados if 500 <= p <= 100000]
                if precios_tipicos:
                    return precios_tipicos[0]
                return precios_filtrados[0]
    
    except Exception as e:
        print(f"Error en estrategia de texto completo: {e}")
    
    return None

def estrategia_javascript(driver: webdriver.Chrome) -> Optional[int]:
    """
    Usa JavaScript para buscar precios en el DOM, incluyendo datos estructurados.
    """
    try:
        # Buscar datos estructurados (JSON-LD)
        script = """
        // Buscar JSON-LD
        const scripts = document.querySelectorAll('script[type="application/ld+json"]');
        for (let script of scripts) {
            try {
                const data = JSON.parse(script.textContent);
                if (data.offers && data.offers.price) {
                    return parseFloat(data.offers.price);
                }
                if (data.price) {
                    return parseFloat(data.price);
                }
                if (data['@graph']) {
                    for (let item of data['@graph']) {
                        if (item.offers && item.offers.price) {
                            return parseFloat(item.offers.price);
                        }
                    }
                }
            } catch (e) {}
        }
        
        // Buscar en meta tags
        const metaPrice = document.querySelector('meta[property="product:price:amount"]');
        if (metaPrice) {
            return parseFloat(metaPrice.content);
        }
        
        // Buscar en atributos data
        const elements = document.querySelectorAll('[data-price], [data-product-price], [data-offer-price]');
        for (let el of elements) {
            const price = el.dataset.price || el.dataset.productPrice || el.dataset.offerPrice;
            if (price) {
                const num = parseFloat(price.replace(/[^0-9]/g, ''));
                if (!isNaN(num) && num > 0) {
                    return num;
                }
            }
        }
        
        return null;
        """
        
        precio = driver.execute_script(script)
        if precio and precio > 0:
            return int(precio)
    
    except Exception as e:
        print(f"Error en estrategia JavaScript: {e}")
    
    return None

def guardar_html_debug(driver: webdriver.Chrome, sku: str, url: str):
    """
    Guarda el HTML de la página para debugging posterior.
    """
    if not DEBUG_MODE:
        return
    
    try:
        # Crear nombre de archivo único
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = DEBUG_DIR / f"{sku}_{url_hash}_{timestamp}.html"
        
        # Guardar HTML
        html = driver.page_source
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"\n")
            f.write(f"\n")
            f.write(f"\n")
            f.write(html)
        
        # También guardar screenshot si es posible
        try:
            screenshot_file = filename.with_suffix('.png')
            driver.save_screenshot(str(screenshot_file))
        except Exception:
            pass
            
        print(f"  Debug: HTML guardado en {filename}")
    
    except Exception as e:
        print(f"  Error guardando HTML debug: {e}")

def obtener_precio_mejorado(url: str, driver: webdriver.Chrome, sku: str = "", timeout_s: int = 20, retries: int = 2) -> Tuple[Optional[int], str]:
    """
    Versión mejorada que usa múltiples estrategias para extraer el precio.
    """
    last_err = ""
    
    for intento in range(1, retries + 2):
        try:
            # Cargar la página
            driver.get(url)
            
            # Esperar carga completa
            esperar_carga_completa(driver, timeout_s)
            
            # Estrategia 1: JavaScript (datos estructurados)
            precio = estrategia_javascript(driver)
            if precio and precio > 0:
                return precio, "javascript"
            
            # Estrategia 2: Selectores CSS mejorados
            precio = estrategia_selectores_mejorada(driver)
            if precio and precio > 0:
                return precio, "selectores"
            
            # Estrategia 3: Análisis de texto completo
            precio = estrategia_texto_completo(driver)
            if precio and precio > 0:
                return precio, "texto_completo"
            
            # Si no se encontró precio, guardar HTML para debugging
            if intento == retries + 1:
                guardar_html_debug(driver, sku, url)
            
            last_err = "precio_no_encontrado"
            
        except TimeoutException:
            last_err = "timeout"
        except Exception as e:
            last_err = f"error:{type(e).__name__}"
            
        # Esperar antes de reintentar
        if intento < retries + 1:
            time.sleep(2.0 + 1.0 * intento)
    
    return None, last_err

# =========================
# Google Sheets helpers (completado)
# =========================

def leer_jumbo_info(sh) -> List[Dict[str, Any]]:
    """Lee todas las filas de 'Jumbo-info' y devuelve una lista de dicts."""
    ws = sh.worksheet(SHEET_JUMBO_INFO)
    values = ws.get_all_values()
    if len(values) < 2:
        return []

    rows = []
    for r in range(2, len(values) + 1):
        row = values[r-1]
        while len(row) < 7:
            row.append("")

        sku = str(row[COL_SKU_INFO-1]).strip()
        url = str(row[COL_URL_INFO-1]).strip()
        peso_j = row[COL_PESO_JUMBO_INFO-1]
        precio_gm = row[COL_PRECIO_GM_INFO-1]
        peso_gm = row[COL_PESO_GM_INFO-1]

        def to_num(x):
            try:
                return float(str(x).replace(",", "."))
            except Exception:
                return None

        rows.append({
            "row_index": r,
            "SKU": sku,
            "URL": url,
            "PesoJumbo_g": to_num(peso_j),
            "PrecioGM": to_num(precio_gm),
            "PesoGM_g": to_num(peso_gm)
        })
    return rows

def mapear_sku_a_fila(ws, col_sku_idx: int) -> Dict[str, int]:
    """Devuelve dict SKU -> row_index (1-based) leyendo una columna de la hoja."""
    values = ws.col_values(col_sku_idx)
    mapping = {}
    for i, v in enumerate(values, start=1):
        if i == 1:
            continue  # header
        sku = str(v).strip()
        if sku:
            mapping[sku] = i
    return mapping

def escribir_pweb(ws_pweb, dict_sku_precio_kg: Dict[str, Optional[int]]):
    """Actualiza P-web (columna I = Jumbo Kg) por SKU, sin pisar si el nuevo valor es None."""
    sku_to_row = mapear_sku_a_fila(ws_pweb, COL_SKU_PWEB)
    updates = []
    for sku, nuevo in dict_sku_precio_kg.items():
        row = sku_to_row.get(sku)
        if not row or row == 1:
            continue
        if nuevo is None or nuevo == "":
            continue  # no pisar si no hay valor
        a1 = f"I{row}"
        updates.append({"range": a1, "values": [[nuevo]]})
    if updates:
        ws_pweb.batch_update(updates)

def escribir_jumbo_historico(ws_hist, dict_sku_precio_kg: Dict[str, Optional[int]], fecha_str: str):
    """Agrega una columna nueva con la fecha y escribe por SKU los valores (usando batch_update seguro)."""
    sku_to_row = mapear_sku_a_fila(ws_hist, COL_SKU_HIST)

    # Determinar próxima columna disponible (>= C)
    values = ws_hist.get_all_values()
    if not values:
        values = [[""]]
    num_cols = max(len(r) for r in values) if values else 1
    new_col_idx = num_cols + 1 if num_cols >= COL_FECHAS_INICIA_EN else COL_FECHAS_INICIA_EN

    # Encabezado de fecha en la fila 1 (usar batch_update con [[valor]], evita errores 400)
    header_a1 = f"{col_idx_to_letter(new_col_idx)}1"
    ws_hist.batch_update([{"range": header_a1, "values": [[fecha_str]]}])

    # Agregar SKUs que no existan
    to_append = []
    for sku in dict_sku_precio_kg.keys():
        if sku and sku not in sku_to_row:
            to_append.append(["", sku])  # col A vacío, col B = SKU
    if to_append:
        ws_hist.append_rows(to_append, value_input_option="RAW")
        sku_to_row = mapear_sku_a_fila(ws_hist, COL_SKU_HIST)

    # Escribir valores en la nueva columna
    updates = []
    for sku, val in dict_sku_precio_kg.items():
        r = sku_to_row.get(sku)
        if not r or r == 1:
            continue
        a1 = f"{col_idx_to_letter(new_col_idx)}{r}"
        updates.append({"range": a1, "values": [[ "" if val is None else val ]]})
    if updates:
        ws_hist.batch_update(updates)

def col_idx_to_letter(idx: int) -> str:
    """Convierte índice de columna (1-based) a letra tipo A1."""
    letters = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters

# =========================
# Flujo principal (completado)
# =========================

def main():
    # Fecha local America/Santiago
    tz_scl = tz.gettz("America/Santiago")
    fecha_str = datetime.now(tz_scl).strftime("%d-%m-%Y")

    sh = open_sheet()
    ws_pweb = sh.worksheet(SHEET_PWEB)
    ws_hist = sh.worksheet(SHEET_JUMBO_HIST)

    productos = leer_jumbo_info(sh)
    if not productos:
        print("No hay filas en Jumbo-info.")
        return

    print(f"Filas a procesar: {len(productos)}")
    if DEBUG_MODE:
        print(f"MODO DEBUG ACTIVADO: Los HTML de las páginas fallidas se guardarán en '{DEBUG_DIR}/'")

    driver = build_browser()
    dict_sku_precio_kg_jumbo: Dict[str, Optional[int]] = {}
    stats = {"ok": 0, "javascript": 0, "selectores": 0, "texto_completo": 0, "fallidos": 0}

    try:
        for i, item in enumerate(productos, start=1):
            sku = item["SKU"]
            url = item["URL"]
            peso_j = item["PesoJumbo_g"]

            print(f"Procesando {i}/{len(productos)}: SKU {sku}")

            if not sku or not url or not peso_j or float(peso_j) <= 0:
                print(f"  -> Omitido: datos incompletos (SKU: {sku}, URL: {url}, Peso: {peso_j})")
                dict_sku_precio_kg_jumbo[sku] = None
                stats["fallidos"] += 1
                continue

            precio, status = obtener_precio_mejorado(url, driver, sku=sku)
            if precio is None:
                dict_sku_precio_kg_jumbo[sku] = None
                stats["fallidos"] += 1
                print(f"  -> Falló ({status})")
            else:
                dict_sku_precio_kg_jumbo[sku] = precio_por_kg(precio, peso_j)
                stats["ok"] += 1
                if status in stats:
                    stats[status] +=1
                print(f"  -> Éxito: ${precio} (método: {status})")


            if i % 10 == 0:
                print(f"--- Parcial: {i}/{len(productos)} procesados ---")
            
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
    finally:
        driver.quit()

    # 1) Actualizar P-web (columna I), sin pisar valores cuando no hay nuevo
    print("\nActualizando hoja 'P-web'...")
    escribir_pweb(ws_pweb, dict_sku_precio_kg_jumbo)
    print("'P-web' actualizado (columna I / Jumbo Kg).")

    # 2) Actualizar Jumbo (histórico) agregando una nueva columna con la fecha
    print("\nActualizando hoja 'Jumbo' (histórico)...")
    escribir_jumbo_historico(ws_hist, dict_sku_precio_kg_jumbo, fecha_str)
    print(f"'Jumbo' histórico actualizado (fecha: {fecha_str}).")

    # Métricas
    print("\n" + "="*20)
    print("Resumen de Ejecución")
    print("="*20)
    total = len(dict_sku_precio_kg_jumbo)
    con_valor = stats["ok"]
    sin_valor = stats["fallidos"]
    print(f"Total de productos intentados: {total}")
    print(f" -> Con valor (Éxito): {con_valor}")
    print(f" -> Sin valor (Fallo): {sin_valor}")
    print("\nMétodos de extracción exitosos:")
    print(f" - Datos Estructurados (JS): {stats['javascript']}")
    print(f" - Selectores CSS: {stats['selectores']}")
    print(f" - Análisis de Texto: {stats['texto_completo']}")
    print("="*20)


if __name__ == "__main__":
    main()
