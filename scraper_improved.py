# -*- coding: utf-8 -*-
"""
Scraper para obtener el precio por kilo desde Jumbo y
actualizar únicamente la columna I (Jumbo Kg) de la hoja P-web.

El script busca el precio por kg directamente del DOM de Jumbo.
Si no lo encuentra explícitamente, calcula el precio por kg
dividiendo el precio unitario por el peso proporcionado en Jumbo-info.

Variables de entorno necesarias:

* ``SHEET_ID`` – ID del spreadsheet a actualizar.
* ``GCP_SHEETS_CREDENTIALS`` – JSON de la cuenta de servicio que
  permite escribir en la hoja de cálculo.
* ``CHROME_BIN`` – opcional; ruta al binario de Chrome si se quiere
  usar una versión específica.
"""

from __future__ import annotations

import os
import re
import time
import json
import random
import uuid
import tempfile
from typing import Optional, Tuple, List, Dict, Any

import gspread  # type: ignore
from google.oauth2.service_account import Credentials  # type: ignore

from selenium import webdriver  # type: ignore
from selenium.webdriver.common.by import By  # type: ignore
from selenium.webdriver.chrome.options import Options  # type: ignore
from selenium.webdriver.support.ui import WebDriverWait  # type: ignore
from selenium.webdriver.support import expected_conditions as EC  # type: ignore

# =========================
# Configuración de hojas / columnas
# =========================

SHEET_ID = os.getenv("SHEET_ID", "").strip()
if not SHEET_ID:
    raise RuntimeError("Falta SHEET_ID en variables de entorno.")

# Nombres de las hojas
SHEET_JUMBO_INFO = "Jumbo-info"
SHEET_PWEB = "P-web"

# Jumbo-info: B=SKU, D=URL, E=Peso Jumbo (g)
COL_SKU_INFO = 2
COL_URL_INFO = 4
COL_PESO_JUMBO_INFO = 5

# P-web: B=SKU, I="Jumbo Kg"
COL_SKU_PWEB = 2
COL_JUMBO_KG_PWEB = 9  # Columna I

SLEEP_MIN = 0.6
SLEEP_MAX = 1.2

# =========================
# Autenticación Google
# =========================

def _get_gspread_client() -> gspread.Client:
    """Autentica contra Google Sheets usando la variable de entorno.

    La variable ``GCP_SHEETS_CREDENTIALS`` debe contener el JSON de
    credenciales de una cuenta de servicio con permiso para editar el
    spreadsheet.  Devuelve un cliente autorizado de gspread.
    """
    creds_json = os.getenv("GCP_SHEETS_CREDENTIALS", "")
    if not creds_json:
        raise RuntimeError(
            "Falta GCP_SHEETS_CREDENTIALS en variables de entorno (pegar JSON completo)."
        )
    info = json.loads(creds_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(credentials)


def open_sheet() -> gspread.Spreadsheet:
    """Abre el spreadsheet indicado por ``SHEET_ID``."""
    gc = _get_gspread_client()
    return gc.open_by_key(SHEET_ID)


# =========================
# Utilidades de precio
# =========================

# Coincide con patrones de precio unitario (ej.: "$ 7.990" o "CLP 7.990").
PRECIO_REGEXES = [
    re.compile(r"\$\s*([\d\.]+)"),  # $ 7.990
    re.compile(r"CLP\s*([\d\.]+)", re.IGNORECASE),  # CLP 7.990
]

# Palabras que invalidan un texto como candidato a precio unitario
PALABRAS_EXCLUIR = {"prime", "paga", "antes", "suscríbete", "suscribete"}


def normaliza(texto: str) -> str:
    """Normaliza el texto eliminando espacios repetidos y espacios extremos."""
    return " ".join(str(texto).split()).strip()


def extraer_precio(texto: str) -> Optional[int]:
    """Intenta extraer un valor entero desde un string que contenga un precio.

    Recibe el texto crudo (por ejemplo, "$ 7.990") y devuelve el número
    entero sin puntos ni comas, o ``None`` si no hay coincidencias.
    """
    t = texto.strip()
    for rx in PRECIO_REGEXES:
        m = rx.search(t)
        if m:
            bruto = m.group(1).replace(".", "").replace(",", "")
            if bruto.isdigit():
                return int(bruto)
    return None


def es_precio_valido(txt: str) -> bool:
    """Determina si un texto es un precio unitario válido.

    Se descartan los textos que no contienen ``$`` o ``CLP``, aquellos que
    incluyen palabras excluidas o que comienzan con «antes», «normal» o
    «precio normal», ya que suelen referirse a precios antiguos o
    promocionales sin utilidad para nuestro cálculo.
    """
    t = txt.lower()
    if not ("$" in t or "clp" in t):
        return False
    if any(p in t for p in PALABRAS_EXCLUIR):
        return False
    if t.startswith(("antes", "normal", "precio normal")):
        return False
    return True


def precio_por_kg(precio: Optional[int], peso_gr: Optional[float]) -> Optional[int]:
    """Calcula el precio por kilo a partir de un precio unitario y un peso en gramos.

    Si alguno de los argumentos es ``None`` o el peso es cero o negativo,
    devuelve ``None``.  De lo contrario, divide ``precio`` por ``peso_gr``
    y lo multiplica por 1000 para llevarlo a kg, redondeando al entero más
    cercano.
    """
    if precio is None or peso_gr is None:
        return None
    try:
        peso_gr = float(peso_gr)
        if peso_gr <= 0:
            return None
        return round(int(precio) / peso_gr * 1000)
    except Exception:
        return None


# Expresión regular para extraer el precio por kg que aparece explícito en la página.
PRECIO_POR_KG_REGEX = re.compile(
    r"\$?\s*([\d\.,]+)\s*(?:x|/)\s*kg", re.IGNORECASE
)


def extraer_precio_por_kg(texto: str) -> Optional[int]:
    """Intenta extraer el precio por kg de un texto que contenga «/kg» o «x kg».

    Busca patrones como ``$7.990/kg``, ``$16.990 x kg`` o ``12000/kg``.  Si
    encuentra un número, lo devuelve como entero (sin separadores de miles
    ni decimales).  Retorna ``None`` si no hay coincidencia.
    """
    m = PRECIO_POR_KG_REGEX.search(texto)
    if m:
        valor = m.group(1).replace(".", "").replace(",", "")
        if valor.isdigit():
            return int(valor)
    return None


# =========================
# Selenium (Chrome headless) con perfil único
# =========================

def build_browser() -> webdriver.Chrome:
    """Construye una instancia headless de Chrome con un perfil único.

    Se intenta utilizar el binario de Chrome especificado en la variable
    ``CHROME_BIN``; de lo contrario, Selenium Manager elegirá el driver
    adecuado.  También se deshabilitan varias características que no
    aportan al scraping y que podrían afectar el rendimiento.
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
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    # Crear un perfil único por ejecución para evitar conflictos
    profile_dir = os.path.join(tempfile.gettempdir(), f"chrome-profile-{uuid.uuid4()}")
    os.makedirs(profile_dir, exist_ok=True)
    options.add_argument(f"--user-data-dir={profile_dir}")
    options.add_argument("--remote-debugging-port=9222")

    chrome_bin = os.environ.get("CHROME_BIN", "").strip()
    if chrome_bin and os.path.exists(chrome_bin):
        options.binary_location = chrome_bin

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(90)
    return driver


def encontrar_precios_en_dom(driver: webdriver.Chrome) -> Tuple[Optional[int], Optional[int]]:
    """Busca en el DOM tanto el precio unitario como el precio por kilo.

    Recorre diferentes selectores CSS que contienen la palabra «price» y
    también elementos genéricos como ``span``, ``div``, ``p``, ``strong``,
    ``b`` y ``li``.  Normaliza el texto extraído y aplica las funciones de
    extracción para cada caso.  Devuelve una tupla `(precio_unitario,
    precio_por_kg)`, donde cualquiera de ellos puede ser ``None`` si no se
    encuentra.
    """
    textos: List[str] = []
    selectores_css = [
        "[class*='price']",
        "[data-testid*='price']",
        "[data-qa*='price']",
        ".price, .product-price, .sale-price, .current-price, .pricing, .skuBestPrice, .productBestPrice",
        "span, div, p, strong, b, li",
    ]
    for sel in selectores_css:
        try:
            for e in driver.find_elements(By.CSS_SELECTOR, sel):
                txt = normaliza(e.text)
                if txt:
                    textos.append(txt)
        except Exception:
            # Algunos selectores podrían fallar si no son válidos; continuamos.
            continue

    precio_unitario: Optional[int] = None
    precio_kg: Optional[int] = None

    for t in textos:
        # Primero intentamos extraer el precio por kg explícito.
        if precio_kg is None:
            pk = extraer_precio_por_kg(t)
            if pk is not None and pk > 0:
                precio_kg = pk
        # Luego el precio unitario (evitamos sobrescribir si ya se encontró)
        if precio_unitario is None and es_precio_valido(t):
            p = extraer_precio(t)
            if p is not None and p > 0:
                precio_unitario = p
        # Si tenemos ambos, no hace falta seguir buscando
        if precio_unitario is not None and precio_kg is not None:
            break

    return precio_unitario, precio_kg


def obtener_precios(url: str, driver: webdriver.Chrome, timeout_s: int = 15, retries: int = 2) -> Tuple[Optional[int], Optional[int], str]:
    """Navega a una URL y devuelve los precios encontrados.

    Devuelve una tupla `(precio_unitario, precio_por_kg, status)` donde
    ``status`` describe brevemente el resultado ("ok", "precio_no_encontrado"
    o un mensaje de error).  Intenta varias veces en caso de fallos
    transitorios.
    """
    last_err = ""
    for intento in range(1, retries + 2):
        try:
            driver.get(url)
            # Esperar a que exista algún signo de precio en la página
            try:
                WebDriverWait(driver, timeout_s).until(
                    EC.presence_of_element_located((By.XPATH, "//*[contains(., '$')]"))
                )
            except Exception:
                pass
            precio_unit, precio_kg = encontrar_precios_en_dom(driver)
            if (precio_unit and precio_unit > 0) or (precio_kg and precio_kg > 0):
                return precio_unit, precio_kg, "ok"
            last_err = "precio_no_encontrado"
        except Exception as e:
            last_err = f"error_navegacion:{type(e).__name__}"
        # Esperar un poco más en cada intento
        time.sleep(1.0 + 0.5 * intento)
    return None, None, last_err or "desconocido"


# =========================
# Google Sheets helpers
# =========================

def leer_jumbo_info(sh: gspread.Spreadsheet) -> List[Dict[str, Any]]:
    """Lee todas las filas de la hoja «Jumbo-info» y devuelve una lista de dicts.

    Cada diccionario contiene las claves ``row_index``, ``SKU``, ``URL`` y
    ``PesoJumbo_g``. Los valores que no se puedan convertir a número quedan
    como ``None``.
    """
    ws = sh.worksheet(SHEET_JUMBO_INFO)
    values = ws.get_all_values()
    if len(values) < 2:
        return []

    rows: List[Dict[str, Any]] = []
    for r in range(2, len(values) + 1):
        row = values[r - 1]
        # Asegurarse de que haya suficientes columnas
        while len(row) < 5:
            row.append("")

        sku = str(row[COL_SKU_INFO - 1]).strip()
        url = str(row[COL_URL_INFO - 1]).strip()
        peso_j = row[COL_PESO_JUMBO_INFO - 1]

        def to_num(x: Any) -> Optional[float]:
            try:
                # Reemplazar coma por punto para soportar formatos "1,5"
                return float(str(x).replace(",", "."))
            except Exception:
                return None

        rows.append(
            {
                "row_index": r,
                "SKU": sku,
                "URL": url,
                "PesoJumbo_g": to_num(peso_j),
            }
        )
    return rows


def mapear_sku_a_fila(ws: gspread.Worksheet, col_sku_idx: int) -> Dict[str, int]:
    """Devuelve un diccionario SKU -> row_index (1‑based) leyendo una columna de la hoja."""
    values = ws.col_values(col_sku_idx)
    mapping: Dict[str, int] = {}
    for i, v in enumerate(values, start=1):
        if i == 1:
            continue  # header
        sku = str(v).strip()
        if sku:
            mapping[sku] = i
    return mapping


def escribir_pweb(ws_pweb: gspread.Worksheet, dict_sku_precio_kg: Dict[str, Optional[int]]) -> None:
    """Actualiza P‑web (columna I = Jumbo Kg) por SKU.

    Solo escribe valores nuevos (no sobreescribe valores existentes si el
    nuevo valor es ``None`` o cadena vacía).  Utiliza ``batch_update`` para
    minimizar la cantidad de llamadas a la API.
    """
    sku_to_row = mapear_sku_a_fila(ws_pweb, COL_SKU_PWEB)
    updates: List[Dict[str, Any]] = []
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


# =========================
# Flujo principal
# =========================

def main() -> None:
    sh = open_sheet()
    ws_pweb = sh.worksheet(SHEET_PWEB)

    productos = leer_jumbo_info(sh)
    if not productos:
        print("No hay filas en Jumbo-info.")
        return

    print(f"Filas a procesar: {len(productos)}")

    driver = build_browser()
    dict_sku_precio_kg_jumbo: Dict[str, Optional[int]] = {}

    try:
        for i, item in enumerate(productos, start=1):
            sku = item.get("SKU")
            url = item.get("URL")
            peso_j = item.get("PesoJumbo_g")

            if not sku or not url:
                dict_sku_precio_kg_jumbo[sku] = None
                continue

            precio_unit, precio_kg_encontrado, status = obtener_precios(url, driver)
            if precio_kg_encontrado is not None:
                # Si encontramos el precio por kg directamente de la página lo usamos.
                dict_sku_precio_kg_jumbo[sku] = precio_kg_encontrado
                print(f"SKU {sku}: Precio/kg encontrado directamente: ${precio_kg_encontrado}")
            else:
                # Si no, calculamos usando el peso proporcionado.
                valor = precio_por_kg(precio_unit, peso_j)
                dict_sku_precio_kg_jumbo[sku] = valor
                if valor:
                    print(f"SKU {sku}: Precio/kg calculado: ${valor}")
                else:
                    print(f"SKU {sku}: No se pudo obtener precio/kg")

            if i % 10 == 0:
                print(f"Procesados {i}/{len(productos)}")
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
    finally:
        driver.quit()

    # Actualizar P-web (columna I), sin pisar valores cuando no hay nuevo
    escribir_pweb(ws_pweb, dict_sku_precio_kg_jumbo)
    print("P-web actualizado (columna I / Jumbo Kg).")

    # Métricas
    total = len(dict_sku_precio_kg_jumbo)
    con_valor = sum(1 for v in dict_sku_precio_kg_jumbo.values() if v is not None)
    sin_valor = total - con_valor
    print(f"Resumen: total={total}, con_valor={con_valor}, sin_valor={sin_valor}")


if __name__ == "__main__":
    main()
