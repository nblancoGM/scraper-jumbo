# -*- coding: utf-8 -*-
"""
Scraper + actualización de Google Sheets (P-web y Jumbo) vía API (gspread).

VERSIÓN CORREGIDA Y ROBUSTA PARA CI/CD (GITHUB ACTIONS)

Lee hoja 'Jumbo-info' por posición de columna para evitar errores de cabecera.
(SKU=Col B, URL=Col D, Peso Jumbo=Col E).

Scrapea el precio en la página de Jumbo con Selenium (Chrome headless) usando
una pausa fija, que ha demostrado ser el método más efectivo.

Actualiza:
  * Hoja 'P-web' -> Columna I ("Jumbo Kg").
  * Hoja 'Jumbo' (histórico) -> Agrega columna con fecha dd-mm-YYYY.

Variables de entorno requeridas:
- GCP_SHEETS_CREDENTIALS  (contenido JSON de Service Account)
- SHEET_ID                (ID del spreadsheet)
- CHROME_BIN              (Ruta al binario de Chrome, ej: /usr/bin/google-chrome)
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
from selenium.common.exceptions import WebDriverException

# =========================
# Configuración de hojas / columnas
# =========================

SHEET_ID = os.getenv("SHEET_ID", "").strip()
if not SHEET_ID:
    raise RuntimeError("Falta SHEET_ID en variables de entorno.")

SHEET_JUMBO_INFO = "Jumbo-info"
SHEET_PWEB = "P-web"
SHEET_JUMBO_HIST = "Jumbo"

# Posiciones de columnas (1-based index)
COL_SKU_INFO = 2           # Columna B
COL_URL_INFO = 4           # Columna D
COL_PESO_JUMBO_INFO = 5    # Columna E

COL_SKU_PWEB = 2           # Columna B
COL_JUMBO_KG_PWEB = 9      # Columna I

COL_SKU_HIST = 2           # Columna B
COL_FECHAS_INICIA_EN = 3   # Columna C

# Tiempos de espera para no sobrecargar el servidor y evitar bloqueos
SLEEP_MIN = 2.0
SLEEP_MAX = 3.5

# =========================
# Autenticación Google
# =========================

def _get_gspread_client():
    creds_json = os.getenv("GCP_SHEETS_CREDENTIALS", "")
    if not creds_json:
        raise RuntimeError("Falta GCP_SHEETS_CREDENTIALS en variables de entorno.")
    try:
        info = json.loads(creds_json)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        credentials = Credentials.from_service_account_info(info, scopes=scopes)
        return gspread.authorize(credentials)
    except json.JSONDecodeError:
        raise RuntimeError("El contenido de GCP_SHEETS_CREDENTIALS no es un JSON válido.")

def open_sheet():
    gc = _get_gspread_client()
    return gc.open_by_key(SHEET_ID)

# =========================
# Utilidades de precio
# =========================

def extraer_precio(texto: str) -> Optional[int]:
    if not texto: return None
    match = re.search(r"\$[\s]?([\d\.]+)", texto)
    if match:
        precio_str = match.group(1).replace(".", "").replace(",", "")
        if precio_str.isdigit():
            return int(precio_str)
    return None

def precio_por_kg(precio: Optional[int], peso_gr: Optional[float]) -> Optional[int]:
    if precio is None or peso_gr is None or peso_gr <= 0:
        return None
    try:
        return round(int(precio) / float(peso_gr) * 1000)
    except (ValueError, TypeError):
        return None

# =========================
# Selenium (Chrome headless)
# =========================

def build_browser():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    profile_dir = os.path.join(tempfile.gettempdir(), f"chrome-profile-{uuid.uuid4()}")
    os.makedirs(profile_dir, exist_ok=True)
    options.add_argument(f"--user-data-dir={profile_dir}")

    chrome_bin = os.environ.get("CHROME_BIN", "").strip()
    if chrome_bin and os.path.exists(chrome_bin):
        print(f"Usando binario de Chrome desde CHROME_BIN: {chrome_bin}")
        options.binary_location = chrome_bin
    else:
        print("CHROME_BIN no definido o no encontrado, dejando que Selenium Manager decida.")

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver

def obtener_precio(url: str, driver: webdriver.Chrome) -> Tuple[Optional[int], str]:
    try:
        driver.get(url)
        # Pausa fija: simple, pero la más efectiva para este caso.
        # Aumentada ligeramente para dar más margen en el entorno de CI.
        pausa = random.uniform(2.5, 4.0)
        print(f"    -> Esperando {pausa:.1f} segundos para carga de JS...")
        time.sleep(pausa)

        spans = driver.find_elements(By.TAG_NAME, 'span')
        print(f"    -> Encontrados {len(spans)} elementos <span>.")

        posibles_precios = []
        for span in spans:
            txt = span.text.strip()
            if "$" in txt and len(txt) < 30: # Filtrar textos largos
                posibles_precios.append(txt)
                # Buscar el que tiene 'font-bold' para priorizarlo
                if 'font-bold' in span.get_attribute('class'):
                    if not ("paga" in txt.lower() or "prime" in txt.lower()):
                        precio = extraer_precio(txt)
                        if precio and precio > 0:
                            print(f"    -> ✅ Precio prioritario (font-bold) encontrado: '{txt}'")
                            return precio, "ok"

        # Si no se encontró uno prioritario, revisar los demás candidatos
        print(f"    -> Revisando {len(posibles_precios)} candidatos de precio...")
        for txt in posibles_precios:
            if not ("paga" in txt.lower() or "prime" in txt.lower()):
                precio = extraer_precio(txt)
                if precio and precio > 0:
                    print(f"    -> ✅ Precio candidato válido encontrado: '{txt}'")
                    return precio, "ok"

    except WebDriverException as e:
        print(f"    -> ❌ Error de WebDriver: {e.msg}")
        return None, f"error_navegacion:{type(e).__name__}"
    except Exception as e:
        print(f"    -> ❌ Error inesperado: {str(e)}")
        return None, f"error_inesperado:{type(e).__name__}"

    return None, "precio_no_encontrado"

# =========================
# Google Sheets helpers
# =========================

def leer_jumbo_info(sh) -> List[Dict[str, Any]]:
    """
    Lee 'Jumbo-info' usando índices de columna para máxima robustez.
    """
    ws = sh.worksheet(SHEET_JUMBO_INFO)
    values = ws.get_all_values()
    if len(values) < 2: return [] # Si no hay datos además de la cabecera

    rows = []
    # Empezar desde la segunda fila (índice 1) para saltar la cabecera
    for i, row_list in enumerate(values[1:], start=2):
        # Asegurarse de que la fila tiene suficientes columnas
        if len(row_list) < COL_PESO_JUMBO_INFO:
            continue

        sku = str(row_list[COL_SKU_INFO - 1]).strip()
        url = str(row_list[COL_URL_INFO - 1]).strip()
        peso_j_str = str(row_list[COL_PESO_JUMBO_INFO - 1]).strip()

        def to_num(x_str):
            if not x_str: return None
            try:
                return float(x_str.replace(",", "."))
            except (ValueError, TypeError):
                return None

        rows.append({
            "row_index": i,
            "SKU": sku,
            "URL": url,
            "PesoJumbo_g": to_num(peso_j_str),
        })
    return rows


def mapear_sku_a_fila(ws, col_sku_idx: int) -> Dict[str, int]:
    values = ws.col_values(col_sku_idx)
    mapping = {str(v).strip(): i for i, v in enumerate(values, start=1) if v and str(v).strip()}
    return mapping

def escribir_pweb(ws_pweb, dict_sku_precio_kg: Dict[str, Optional[int]]):
    print("Mapeando SKUs en 'P-web'...")
    sku_to_row = mapear_sku_a_fila(ws_pweb, COL_SKU_PWEB)
    updates = []
    for sku, nuevo in dict_sku_precio_kg.items():
        if not sku or nuevo is None: continue
        row = sku_to_row.get(sku)
        if row:
            updates.append({"range": f"I{row}", "values": [[nuevo]]})
    if updates:
        print(f"Enviando {len(updates)} actualizaciones a 'P-web'...")
        ws_pweb.batch_update(updates)
    else:
        print("No hay actualizaciones para 'P-web'.")

def escribir_jumbo_historico(ws_hist, dict_sku_precio_kg: Dict[str, Optional[int]], fecha_str: str):
    print("Mapeando SKUs en 'Jumbo' (histórico)...")
    sku_to_row = mapear_sku_a_fila(ws_hist, COL_SKU_HIST)

    values = ws_hist.get_all_values()
    num_cols = max(len(r) for r in values) if values else 0
    new_col_idx = max(num_cols + 1, COL_FECHAS_INICIA_EN)
    
    header_a1 = f"{col_idx_to_letter(new_col_idx)}1"
    print(f"Agregando encabezado de fecha '{fecha_str}' en la celda {header_a1}...")
    ws_hist.batch_update([{"range": header_a1, "values": [[fecha_str]]}])

    # Agregar SKUs nuevos y preparar actualizaciones en una sola pasada
    updates = []
    skus_a_anadir = []
    for sku, val in dict_sku_precio_kg.items():
        if not sku: continue
        valor_celda = val if val is not None else ""
        if sku in sku_to_row:
            r = sku_to_row[sku]
            a1 = f"{col_idx_to_letter(new_col_idx)}{r}"
            updates.append({"range": a1, "values": [[valor_celda]]})
        else:
            skus_a_anadir.append(["", sku] + [""] * (new_col_idx - 2) + [valor_celda])

    if skus_a_anadir:
        print(f"Agregando {len(skus_a_anadir)} SKUs nuevos al histórico...")
        ws_hist.append_rows(skus_a_anadir, value_input_option="USER_ENTERED")
    
    if updates:
        print(f"Enviando {len(updates)} actualizaciones a SKUs existentes en 'Jumbo'...")
        ws_hist.batch_update(updates)

def col_idx_to_letter(idx: int) -> str:
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
    productos = leer_jumbo_info(sh)

    if not productos:
        print("No se encontraron filas con datos válidos en la hoja 'Jumbo-info'. El script finalizará.")
        return

    print(f"Se encontraron {len(productos)} productos para procesar.")
    driver = build_browser()
    dict_sku_precio_kg_jumbo: Dict[str, Optional[int]] = {}

    try:
        for i, item in enumerate(productos, start=1):
            sku, url, peso_j = item["SKU"], item["URL"], item["PesoJumbo_g"]
            print(f"\n[{i}/{len(productos)}] Procesando SKU: {sku or 'N/A'}")

            if not sku or not url or not peso_j:
                print(f"  -> Datos incompletos en la hoja. SKU='{sku}', URL='{url}', Peso='{peso_j}'. Saltando.")
                if sku: dict_sku_precio_kg_jumbo[sku] = None
                continue
            
            precio, status = obtener_precio(url, driver)
            
            if precio:
                dict_sku_precio_kg_jumbo[sku] = precio_por_kg(precio, peso_j)
            else:
                print(f"  -> ❌ No se encontró precio final. Estado: {status}")
                dict_sku_precio_kg_jumbo[sku] = None
            
            # Pausa entre cada petición
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    finally:
        print("\n--- Cerrando navegador ---")
        driver.quit()

    print("\n--- Actualizando Google Sheets ---")
    ws_pweb = sh.worksheet(SHEET_PWEB)
    ws_hist = sh.worksheet(SHEET_JUMBO_HIST)
    
    escribir_pweb(ws_pweb, dict_sku_precio_kg_jumbo)
    print("✅ 'P-web' actualizado.")

    escribir_jumbo_historico(ws_hist, dict_sku_precio_kg_jumbo, fecha_str)
    print(f"✅ 'Jumbo' histórico actualizado.")

    total = len(dict_sku_precio_kg_jumbo)
    con_valor = sum(1 for v in dict_sku_precio_kg_jumbo.values() if v is not None)
    print("\n--- Resumen Final ---")
    print(f"Total de SKUs procesados: {total}")
    print(f"Con valor: {con_valor}")
    print(f"Sin valor: {total - con_valor}")
    print("--------------------")

if __name__ == "__main__":
    main()
