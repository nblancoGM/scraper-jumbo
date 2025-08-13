# -*- coding: utf-8 -*-
"""
Scraper + actualización de Google Sheets (P-web y Jumbo) vía API (gspread).

Este script combina la estructura de conexión a Google Sheets con el método de
scraping efectivo que espera un tiempo fijo y busca por la clase 'font-bold'.

Lee hoja 'Jumbo-info' (SKU=B, URL=D, Peso Jumbo=E).
Scrapea el precio en la página de Jumbo con Selenium (Chrome headless).
Calcula "Precio por 1 kg Jumbo" = precio_scrapeado / PesoJumbo_g * 1000.

Actualiza:
  * Hoja 'P-web' -> Columna I ("Jumbo Kg"), por SKU.
  * Hoja 'Jumbo' (histórico) -> agrega columna con fecha dd-mm-YYYY, por SKU.

Variables de entorno requeridas:
- GCP_SHEETS_CREDENTIALS  (contenido JSON de Service Account)
- SHEET_ID                (ID del spreadsheet)
- CHROME_BIN              (opcional; si viene, lo usamos como binario de Chrome)

Ejecución:
    python scraper.py
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
from datetime import datetime
from dateutil import tz

import gspread
from google.oauth2.service_account import Credentials

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# =========================
# Configuración de hojas / columnas
# =========================

SHEET_ID = os.getenv("SHEET_ID", "").strip()
if not SHEET_ID:
    raise RuntimeError("Falta SHEET_ID en variables de entorno.")

SHEET_JUMBO_INFO = "Jumbo-info"
SHEET_PWEB = "P-web"
SHEET_JUMBO_HIST = "Jumbo"

# Jumbo-info: B=SKU, D=URL, E=Peso Jumbo (g)
COL_SKU_INFO = 2
COL_URL_INFO = 4
COL_PESO_JUMBO_INFO = 5

# P-web: B=SKU, I="Jumbo Kg"
COL_SKU_PWEB = 2
COL_JUMBO_KG_PWEB = 9  # Columna I

# Jumbo (histórico): B=SKU, columnas de fechas a partir de C
COL_SKU_HIST = 2
COL_FECHAS_INICIA_EN = 3  # Columna C

SLEEP_MIN = 0.8
SLEEP_MAX = 1.5

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
# Utilidades de precio (Método simple y efectivo)
# =========================

def extraer_precio(texto: str) -> Optional[int]:
    """Extrae un precio numérico de un texto como '$7.990'."""
    if not texto:
        return None
    # Busca el patrón de precio como $ 7.990 o $7.990
    match = re.search(r"\$[\s]?([\d\.]+)", texto)
    if match:
        precio_str = match.group(1).replace(".", "").replace(",", "")
        if precio_str.isdigit():
            return int(precio_str)
    return None

def precio_por_kg(precio: Optional[int], peso_gr: Optional[float]) -> Optional[int]:
    """Calcula el precio por 1000 gramos (1 kg)."""
    if precio is None or peso_gr is None:
        return None
    try:
        peso_gr_float = float(peso_gr)
        if peso_gr_float <= 0:
            return None
        return round(int(precio) / peso_gr_float * 1000)
    except (ValueError, TypeError):
        return None

# =========================
# Selenium (Chrome headless) con perfil único
# =========================

def build_browser():
    """Configura el navegador Chrome en modo headless."""
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    # Evita la detección de automatización
    options.add_argument("--disable-blink-features=AutomationControlled")

    # Perfil temporal para evitar conflictos
    profile_dir = os.path.join(tempfile.gettempdir(), f"chrome-profile-{uuid.uuid4()}")
    os.makedirs(profile_dir, exist_ok=True)
    options.add_argument(f"--user-data-dir={profile_dir}")

    chrome_bin = os.environ.get("CHROME_BIN", "").strip()
    if chrome_bin and os.path.exists(chrome_bin):
        options.binary_location = chrome_bin

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(90)
    return driver

def obtener_precio(url: str, driver: webdriver.Chrome) -> Tuple[Optional[int], str]:
    """
    Scrapea el precio real usando la estrategia simple y efectiva:
    espera fija y búsqueda por clase 'font-bold'.
    """
    try:
        driver.get(url)
        # La pausa fija es clave para dejar que el JS de la página cargue el precio
        time.sleep(2.5)

        # Busca elementos <span> con la clase 'font-bold', que es donde suele estar el precio
        spans = driver.find_elements(By.CLASS_NAME, 'font-bold')

        for span in spans:
            txt = span.text.strip()
            # Filtra para obtener el precio correcto y no el de ofertas "prime" o "paga"
            if "$" in txt and not ("paga" in txt.lower() or "prime" in txt.lower()):
                precio = extraer_precio(txt)
                if precio and precio > 0:
                    return precio, "ok"

    except Exception as e:
        return None, f"error_navegacion:{type(e).__name__}"

    # Si el bucle termina sin encontrar un precio válido
    return None, "precio_no_encontrado"


# =========================
# Google Sheets helpers
# =========================

def leer_jumbo_info(sh) -> List[Dict[str, Any]]:
    """Lee todas las filas de 'Jumbo-info' y devuelve una lista de dicts."""
    ws = sh.worksheet(SHEET_JUMBO_INFO)
    values = ws.get_all_records() # Más robusto que get_all_values
    
    rows = []
    for i, row_dict in enumerate(values, start=2):
        # Asegurarse de que las claves existan, usando .get() con un valor por defecto
        sku = str(row_dict.get("SKU", "")).strip()
        url = str(row_dict.get("URL", "")).strip()
        peso_j = row_dict.get("Peso Jumbo (g)")

        def to_num(x):
            if x is None or x == "": return None
            try:
                return float(str(x).replace(",", "."))
            except (ValueError, TypeError):
                return None

        rows.append({
            "row_index": i,
            "SKU": sku,
            "URL": url,
            "PesoJumbo_g": to_num(peso_j),
        })
    return rows

def mapear_sku_a_fila(ws, col_sku_idx: int) -> Dict[str, int]:
    """Devuelve dict SKU -> row_index (1-based) leyendo una columna de la hoja."""
    values = ws.col_values(col_sku_idx)
    mapping = {}
    for i, v in enumerate(values, start=1):
        sku = str(v).strip()
        if sku:
            mapping[sku] = i
    return mapping

def escribir_pweb(ws_pweb, dict_sku_precio_kg: Dict[str, Optional[int]]):
    """Actualiza P-web (columna I = Jumbo Kg) por SKU, sin pisar si el nuevo valor es None."""
    print("Mapeando SKUs en 'P-web'...")
    sku_to_row = mapear_sku_a_fila(ws_pweb, COL_SKU_PWEB)
    updates = []
    for sku, nuevo in dict_sku_precio_kg.items():
        if not sku: continue
        row = sku_to_row.get(sku)
        if not row or row == 1:
            continue
        if nuevo is None or nuevo == "":
            continue
        a1 = f"I{row}"
        updates.append({"range": a1, "values": [[nuevo]]})

    if updates:
        print(f"Enviando {len(updates)} actualizaciones a 'P-web'...")
        ws_pweb.batch_update(updates)
    else:
        print("No hay actualizaciones para 'P-web'.")

def escribir_jumbo_historico(ws_hist, dict_sku_precio_kg: Dict[str, Optional[int]], fecha_str: str):
    """Agrega una columna nueva con la fecha y escribe los valores por SKU."""
    print("Mapeando SKUs en 'Jumbo' (histórico)...")
    sku_to_row = mapear_sku_a_fila(ws_hist, COL_SKU_HIST)

    values = ws_hist.get_all_values()
    if not values: values = [[""]]
    num_cols = max(len(r) for r in values) if values else 1
    new_col_idx = max(num_cols + 1, COL_FECHAS_INICIA_EN)
    
    header_a1 = f"{col_idx_to_letter(new_col_idx)}1"
    print(f"Agregando encabezado de fecha '{fecha_str}' en la celda {header_a1}...")
    ws_hist.batch_update([{"range": header_a1, "values": [[fecha_str]]}])

    to_append = []
    for sku in dict_sku_precio_kg:
        if sku and sku not in sku_to_row:
            to_append.append(["", sku])

    if to_append:
        print(f"Agregando {len(to_append)} SKUs nuevos al histórico...")
        ws_hist.append_rows(to_append, value_input_option="USER_ENTERED")
        sku_to_row = mapear_sku_a_fila(ws_hist, COL_SKU_HIST)

    updates = []
    for sku, val in dict_sku_precio_kg.items():
        if not sku: continue
        r = sku_to_row.get(sku)
        if not r or r == 1: continue
        a1 = f"{col_idx_to_letter(new_col_idx)}{r}"
        updates.append({"range": a1, "values": [[val if val is not None else ""]]})
    
    if updates:
        print(f"Enviando {len(updates)} actualizaciones a la columna de fecha en 'Jumbo'...")
        ws_hist.batch_update(updates)
    else:
        print("No hay actualizaciones para el histórico 'Jumbo'.")


def col_idx_to_letter(idx: int) -> str:
    """Convierte índice de columna (1-based) a letra tipo A1."""
    letters = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters

# =========================
# Flujo principal
# =========================

def main():
    tz_scl = tz.gettz("America/Santiago")
    fecha_str = datetime.now(tz_scl).strftime("%d-%m-%Y")
    print(f"--- Iniciando scraper - {fecha_str} ---")

    sh = open_sheet()
    ws_pweb = sh.worksheet(SHEET_PWEB)
    ws_hist = sh.worksheet(SHEET_JUMBO_HIST)

    productos = leer_jumbo_info(sh)
    if not productos:
        print("No hay filas válidas en la hoja 'Jumbo-info'.")
        return

    print(f"Se encontraron {len(productos)} productos para procesar.")

    driver = build_browser()
    dict_sku_precio_kg_jumbo: Dict[str, Optional[int]] = {}

    try:
        for i, item in enumerate(productos, start=1):
            sku = item.get("SKU")
            url = item.get("URL")
            peso_j = item.get("PesoJumbo_g")
            
            print(f"\n[{i}/{len(productos)}] Procesando SKU: {sku}")

            if not sku or not url or not peso_j or float(peso_j) <= 0:
                print(f"  -> Datos incompletos. Saltando.")
                dict_sku_precio_kg_jumbo[sku] = None
                continue
            
            print(f"  -> URL: {url}")
            precio, status = obtener_precio(url, driver)

            if precio:
                print(f"  -> ✅ Precio encontrado: ${precio}")
                dict_sku_precio_kg_jumbo[sku] = precio_por_kg(precio, peso_j)
                print(f"  -> ⚖️  Precio/kg: {dict_sku_precio_kg_jumbo[sku]}")
            else:
                print(f"  -> ❌ No se encontró precio. Estado: {status}")
                dict_sku_precio_kg_jumbo[sku] = None

            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
    finally:
        print("\n--- Cerrando navegador ---")
        driver.quit()

    print("\n--- Actualizando Google Sheets ---")
    
    escribir_pweb(ws_pweb, dict_sku_precio_kg_jumbo)
    print("✅ 'P-web' actualizado (columna I / Jumbo Kg).")

    escribir_jumbo_historico(ws_hist, dict_sku_precio_kg_jumbo, fecha_str)
    print(f"✅ 'Jumbo' histórico actualizado ({fecha_str}).")

    total = len(dict_sku_precio_kg_jumbo)
    con_valor = sum(1 for v in dict_sku_precio_kg_jumbo.values() if v is not None)
    sin_valor = total - con_valor
    print("\n--- Resumen Final ---")
    print(f"Total de productos procesados: {total}")
    print(f"Con valor: {con_valor}")
    print(f"Sin valor: {sin_valor}")
    print("--------------------")


if __name__ == "__main__":
    main()
