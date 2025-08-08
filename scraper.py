# -*- coding: utf-8 -*-
"""
Scraper + actualización de Google Sheets (P-web y Jumbo) vía API (gspread).
- Lee hoja 'Jumbo-info' (SKU=B, URL=D, Peso Jumbo=E, Precio GM=F, Peso GM=G)
- Scrapea precio (Jumbo), calcula "Precio por 1 kg Jumbo"
- Actualiza:
  * Hoja 'P-web' -> Columna I ("Jumbo Kg"): solo sobreescribe si hay valor nuevo
  * Hoja 'Jumbo' (histórico) -> agrega nueva columna con fecha dd-mm-YYYY, por SKU
Ejecución: python scraper.py
Variables de entorno requeridas:
- GCP_SHEETS_CREDENTIALS: JSON completo (service account)
- SHEET_ID: ID del spreadsheet
"""

from __future__ import annotations
import os
import re
import time
import json
import random
import shutil
from typing import Optional, Tuple, List, Dict, Any
from datetime import datetime
from dateutil import tz

import gspread
from google.oauth2.service_account import Credentials

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =========================
# Config
# =========================

SHEET_ID = os.getenv("SHEET_ID", "").strip()
if not SHEET_ID:
    raise RuntimeError("Falta SHEET_ID en variables de entorno.")

# nombres de hojas (exactos)
SHEET_JUMBO_INFO = "Jumbo-info"
SHEET_PWEB = "P-web"
SHEET_JUMBO_HIST = "Jumbo"

# Mapeo de columnas fijas por especificación
# Jumbo-info: B=SKU, D=URL, E=Peso Jumbo (g), F=Precio GM, G=Peso GM
COL_SKU_INFO = 2
COL_URL_INFO = 4
COL_PESO_JUMBO_INFO = 5
COL_PRECIO_GM_INFO = 6
COL_PESO_GM_INFO = 7

# P-web: B=SKU, I="Jumbo Kg"
COL_SKU_PWEB = 2
COL_JUMBO_KG_PWEB = 9  # columna I

# Jumbo (histórico): B=SKU, C... fechas (agregar nueva)
COL_SKU_HIST = 2
COL_FECHAS_INICIA_EN = 3  # columna C

# Pausas entre requests para no ser agresivos
SLEEP_MIN = 0.6
SLEEP_MAX = 1.2

# =========================
# Autenticación Google
# =========================

def _get_gspread_client():
    creds_json = os.getenv("GCP_SHEETS_CREDENTIALS", "")
    if not creds_json:
        raise RuntimeError("Falta GCP_SHEETS_CREDENTIALS en variables de entorno.")

    info = json.loads(creds_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    credentials = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(credentials)

# =========================
# Utilidades de precio
# =========================

PRECIO_REGEXES = [
    re.compile(r"\$\s*([\d\.]+)"),      # $ 7.990
    re.compile(r"CLP\s*([\d\.]+)"),     # CLP 7.990
]
PALABRAS_EXCLUIR = {"prime", "paga", "antes", "suscríbete", "suscribete"}

def normaliza(texto: str) -> str:
    return " ".join(texto.split()).strip()

def extraer_precio(texto: str) -> Optional[int]:
    t = texto.strip()
    for rx in PRECIO_REGEXES:
        m = rx.search(t)
        if m:
            bruto = m.group(1).replace(".", "").replace(",", "")
            if bruto.isdigit():
                return int(bruto)
    return None

def es_precio_valido(txt: str) -> bool:
    t = txt.lower()
    if not ("$" in t or "clp" in t):
        return False
    if any(p in t for p in PALABRAS_EXCLUIR):
        return False
    if t.startswith(("antes", "normal", "precio normal")):
        return False
    return True

def precio_por_kg(precio: Optional[int], peso_gr: Optional[float]) -> Optional[int]:
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
# Selenium (Chromium headless)
# =========================

def _chrome_binary_path() -> Optional[str]:
    """
    Detecta el binario de Chrome/Chromium.
    Prioriza variable CHROME_BIN (puede venir del workflow).
    Luego prueba rutas comunes, incluyendo snap.
    """
    env_bin = os.getenv("CHROME_BIN")
    if env_bin and os.path.exists(env_bin):
        return env_bin

    candidates = [
        "/snap/bin/chromium",          # snap (Ubuntu 24.04 GH runner)
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
    ]
    for p in candidates:
        if os.path.exists(p):
            return p

    # si no hay coincidencias, devolvemos None (no forzar una ruta inexistente)
    return None

def _chromedriver_path() -> Optional[str]:
    # Usa el que esté en PATH si existe
    path = shutil.which("chromedriver")
    if path:
        return path
    # rutas típicas
    candidates = [
        "/usr/bin/chromedriver",
        "/usr/lib/chromium-browser/chromedriver",
        "/snap/bin/chromedriver",
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return None  # dejar que Selenium Manager intente resolver

def build_browser() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    bin_path = _chrome_binary_path()
    if bin_path:
        options.binary_location = bin_path

    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1366,768")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    drv_path = _chromedriver_path()
    if drv_path:
        service = Service(executable_path=drv_path)
        driver = webdriver.Chrome(service=service, options=options)
    else:
        # fallback: deja que Selenium Manager resuelva (requiere salida a internet)
        driver = webdriver.Chrome(options=options)

    driver.set_page_load_timeout(25)
    return driver

def encontrar_precio_en_dom(driver: webdriver.Chrome) -> Optional[int]:
    textos = []
    selectores_css = [
        "[class*='price']",
        "[data-testid*='price']",
        "[data-qa*='price']",
        ".price, .product-price, .sale-price, .current-price",
        "span, div, p, strong, b"
    ]
    for sel in selectores_css:
        for e in driver.find_elements(By.CSS_SELECTOR, sel):
            txt = normaliza(e.text)
            if txt:
                textos.append(txt)

    textos = [t for t in textos if es_precio_valido(t)]
    for t in textos:
        p = extraer_precio(t)
        if p is not None and p > 0:
            return p
    return None

def obtener_precio(url: str, driver: webdriver.Chrome, timeout_s: int = 12, retries: int = 2) -> Tuple[Optional[int], str]:
    last_err = ""
    for intento in range(1, retries + 2):
        try:
            driver.get(url)
            try:
                WebDriverWait(driver, timeout_s).until(
                    EC.presence_of_element_located((By.XPATH, "//*[contains(., '$')]"))
                )
            except Exception:
                pass
            precio = encontrar_precio_en_dom(driver)
            if precio and precio > 0:
                return precio, "ok"
            last_err = "precio_no_encontrado"
        except Exception as e:
            last_err = f"error_navegacion:{type(e).__name__}"
        time.sleep(1.0 + 0.5 * intento)
    return None, last_err or "desconocido"

# =========================
# Google Sheets helpers
# =========================

def open_sheet():
    gc = _get_gspread_client()
    return gc.open_by_key(SHEET_ID)

def leer_jumbo_info(sh) -> List[Dict[str, Any]]:
    ws = sh.worksheet(SHEET_JUMBO_INFO)
    values = ws.get_all_values()  # matriz (filas x columnas)
    if len(values) < 2:
        return []
    # Fila 1 cabecera; datos a partir de fila 2
    rows = []
    for r in range(2, len(values) + 1):
        row = values[r-1]
        # expansión de fila a largo mínimo
        while len(row) < 7:
            row.append("")
        sku = str(row[COL_SKU_INFO-1]).strip()
        url = str(row[COL_URL_INFO-1]).strip()
        peso_j = row[COL_PESO_JUMBO_INFO-1]
        precio_gm = row[COL_PRECIO_GM_INFO-1]
        peso_gm = row[COL_PESO_GM_INFO-1]

        # Convertibles
        def to_num(x):
            try:
                return float(str(x).replace(",", "."))
            except Exception:
                return None

        rows.append({
            "row_index": r,  # por si se necesita
            "SKU": sku,
            "URL": url,
            "PesoJumbo_g": to_num(peso_j),
            "PrecioGM": to_num(precio_gm),
            "PesoGM_g": to_num(peso_gm)
        })
    return rows

def mapear_sku_a_fila(ws, col_sku_idx: int) -> Dict[str, int]:
    """ Devuelve dict SKU -> row_index (1-based). """
    values = ws.col_values(col_sku_idx)
    mapping = {}
    for i, v in enumerate(values, start=1):
        sku = str(v).strip()
        if i == 1:
            continue  # header
        if sku:
            mapping[sku] = i
    return mapping

def escribir_pweb(ws_pweb, dict_sku_precio_kg: Dict[str, Optional[int]]):
    # Mapeo SKU -> fila en P-web
    sku_to_row = mapear_sku_a_fila(ws_pweb, COL_SKU_PWEB)

    # Leer valores actuales de la columna I para no pisar cuando None
    col_vals = ws_pweb.col_values(COL_JUMBO_KG_PWEB)
    max_row = ws_pweb.row_count
    while len(col_vals) < max_row:
        col_vals.append("")

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
    # Mapeo actual de SKU -> fila
    sku_to_row = mapear_sku_a_fila(ws_hist, COL_SKU_HIST)

    # Valores de la hoja para calcular el próximo índice de columna
    values = ws_hist.get_all_values()
    if not values:
        values = [[""]]

    num_cols = max(len(r) for r in values) if values else 1
    new_col_idx = num_cols + 1 if num_cols >= COL_FECHAS_INICIA_EN else COL_FECHAS_INICIA_EN

    # Header fecha
    header_a1 = f"{col_idx_to_letter(new_col_idx)}1"
    ws_hist.update(header_a1, fecha_str)

    # Agregar SKUs nuevos (si no existen)
    to_append = []
    for sku in dict_sku_precio_kg.keys():
        if sku and sku not in sku_to_row:
            to_append.append(["", sku])  # A vacío, B = SKU
    if to_append:
        ws_hist.append_rows(to_append, value_input_option="RAW")
        sku_to_row = mapear_sku_a_fila(ws_hist, COL_SKU_HIST)

    # Escribir columna nueva
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
    # 1 -> A, 2 -> B, ...
    letters = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters

# =========================
# Flujo principal
# =========================

def main():
    # Fecha local America/Santiago
    tz_scl = tz.gettz("America/Santiago")
    fecha_str = datetime.now(tz_scl).strftime("%d-%m-%Y")

    sh = open_sheet()
    ws_info = sh.worksheet(SHEET_JUMBO_INFO)
    ws_pweb = sh.worksheet(SHEET_PWEB)
    ws_hist = sh.worksheet(SHEET_JUMBO_HIST)

    productos = leer_jumbo_info(sh)
    if not productos:
        print("No hay filas en Jumbo-info.")
        return

    print(f"Filas a procesar: {len(productos)}")

    driver = build_browser()
    dict_sku_precio_kg_jumbo: Dict[str, Optional[int]] = {}

    for i, item in enumerate(productos, start=1):
        sku = item["SKU"]
        url = item["URL"]
        peso_j = item["PesoJumbo_g"]

        if not sku or not url or not peso_j or float(peso_j) <= 0:
            dict_sku_precio_kg_jumbo[sku] = None
            continue

        precio, status = obtener_precio(url, driver)
        if precio is None:
            dict_sku_precio_kg_jumbo[sku] = None
        else:
            dict_sku_precio_kg_jumbo[sku] = precio_por_kg(precio, peso_j)

        if i % 10 == 0:
            print(f"Procesados {i}/{len(productos)}")
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    driver.quit()

    # 1) Actualizar P-web (columna I), sin pisar valores cuando no hay nuevo
    escribir_pweb(ws_pweb, dict_sku_precio_kg_jumbo)
    print("P-web actualizado (columna I / Jumbo Kg).")

    # 2) Actualizar Jumbo (histórico) agregando una nueva columna con la fecha
    escribir_jumbo_historico(ws_hist, dict_sku_precio_kg_jumbo, fecha_str)
    print(f"Jumbo histórico actualizado ({fecha_str}).")

    # Métricas rápidas
    total = len(dict_sku_precio_kg_jumbo)
    con_valor = sum(1 for v in dict_sku_precio_kg_jumbo.values() if v is not None)
    sin_valor = total - con_valor
    print(f"Resumen: total={total}, con_valor={con_valor}, sin_valor={sin_valor}")

if __name__ == "__main__":
    main()
