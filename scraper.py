# -*- coding: utf-8 -*-
"""
Scraper + actualización de Google Sheets (P-web y Jumbo) vía API (gspread).

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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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

SLEEP_MIN = 1.0
SLEEP_MAX = 2.0

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
# Utilidades de precio (EXACTAMENTE como el código viejo que funcionaba)
# =========================

def extraer_precio(texto):
    """Función EXACTA del código viejo que funcionaba"""
    match = re.search(r"\$[\s]?([\d\.]+)", texto)
    if match:
        return int(match.group(1).replace(".", ""))
    return None

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
# Selenium (Chrome headless) con perfil único
# =========================

def build_browser():
    """
    Chrome headless para CI:
    - Perfil único por ejecución (evita "user data dir is already in use")
    - Usa CHROME_BIN si está definido; Selenium Manager resuelve el driver
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

    profile_dir = os.path.join(tempfile.gettempdir(), f"chrome-profile-{uuid.uuid4()}")
    os.makedirs(profile_dir, exist_ok=True)
    options.add_argument(f"--user-data-dir={profile_dir}")
    options.add_argument("--remote-debugging-port=9222")

    chrome_bin = os.environ.get("CHROME_BIN", "").strip()
    if chrome_bin and os.path.exists(chrome_bin):
        options.binary_location = chrome_bin

    # Selenium Manager elegirá el driver apropiado
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(120)
    return driver

def obtener_precio(url: str, driver: webdriver.Chrome, timeout_s: int = 20, retries: int = 2) -> Tuple[Optional[int], str]:
    """Función EXACTA del código viejo que funcionaba, adaptada para web driver"""
    print(f"🌐 Procesando URL: {url}")
    
    for intento in range(1, retries + 2):
        try:
            print(f"   Intento {intento}")
            driver.get(url)
            
            # Esperar que la página cargue
            time.sleep(3)  # Igual que el código viejo
            
            # Intentar esperar por elementos con precio
            try:
                WebDriverWait(driver, timeout_s).until(
                    EC.presence_of_element_located((By.XPATH, "//*[contains(., '$')]"))
                )
                print("   ✅ Página cargada (encontrado elemento con '$')")
            except Exception:
                print("   ⚠️ Timeout esperando elemento con '$', pero continuando...")
                pass

            # ESTRATEGIA EXACTA DEL CÓDIGO VIEJO: buscar spans con clase 'font-bold'
            spans = driver.find_elements(By.CLASS_NAME, 'font-bold')
            print(f"   🔍 Encontrados {len(spans)} elementos con clase 'font-bold'")
            
            for span in spans:
                txt = span.text.strip()
                print(f"      📝 Texto encontrado: '{txt}'")
                
                # Aplicar EXACTAMENTE la misma lógica del código viejo
                if "$" in txt:
                    print(f"         💰 Contiene '$', verificando si es válido...")
                    
                    # Verificar si NO contiene palabras excluidas (igual que código viejo)
                    txt_lower = txt.lower()
                    if not ("paga" in txt_lower or "prime" in txt_lower):
                        print(f"         ✅ Precio real encontrado: {txt}")
                        precio = extraer_precio(txt)
                        if precio and precio > 0:
                            print(f"         💵 Precio extraído: ${precio:,}")
                            return precio, "ok"
                        else:
                            print(f"         ❌ No se pudo extraer número del precio: {txt}")
                    else:
                        print(f"         ❌ Precio descartado (contiene 'paga' o 'prime'): {txt}")
                else:
                    print(f"         ⚠️ No contiene '$': {txt}")

            print(f"   ❌ No se encontró precio válido en intento {intento}")
                
        except Exception as e:
            print(f"   ❌ Error en intento {intento}: {type(e).__name__}:{str(e)}")
        
        if intento < retries + 1:
            wait_time = 2.0 + 1.0 * intento
            print(f"   ⏳ Esperando {wait_time}s antes del siguiente intento...")
            time.sleep(wait_time)
    
    print(f"   ❌ FALLO FINAL: No se encontró precio después de {retries + 1} intentos")
    return None, "precio_no_encontrado"

# =========================
# Google Sheets helpers
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
    actualizados = 0
    
    for sku, nuevo in dict_sku_precio_kg.items():
        row = sku_to_row.get(sku)
        if not row or row == 1:
            continue
        if nuevo is None or nuevo == "":
            continue  # no pisar si no hay valor
        a1 = f"I{row}"
        updates.append({"range": a1, "values": [[nuevo]]})
        actualizados += 1
    
    if updates:
        ws_pweb.batch_update(updates)
        print(f"✅ P-web actualizado: {actualizados} SKUs")
    else:
        print("⚠️ P-web: no hay valores para actualizar")

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
    valores_escritos = 0
    for sku, val in dict_sku_precio_kg.items():
        r = sku_to_row.get(sku)
        if not r or r == 1:
            continue
        a1 = f"{col_idx_to_letter(new_col_idx)}{r}"
        updates.append({"range": a1, "values": [[ "" if val is None else val ]]})
        if val is not None:
            valores_escritos += 1
            
    if updates:
        ws_hist.batch_update(updates)
        print(f"✅ Histórico actualizado: {valores_escritos} valores en columna {fecha_str}")
    else:
        print("⚠️ Histórico: no hay valores para escribir")

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
    print("🚀 Iniciando scraper de Jumbo...")
    
    # Fecha local America/Santiago
    tz_scl = tz.gettz("America/Santiago")
    fecha_str = datetime.now(tz_scl).strftime("%d-%m-%Y")
    print(f"📅 Fecha: {fecha_str}")

    print("🔗 Conectando a Google Sheets...")
    sh = open_sheet()
    ws_pweb = sh.worksheet(SHEET_PWEB)
    ws_hist = sh.worksheet(SHEET_JUMBO_HIST)

    print("📊 Leyendo productos de Jumbo-info...")
    productos = leer_jumbo_info(sh)
    if not productos:
        print("❌ No hay filas en Jumbo-info.")
        return

    print(f"📦 Productos a procesar: {len(productos)}")

    print("🌐 Iniciando navegador Chrome...")
    driver = build_browser()
    dict_sku_precio_kg_jumbo: Dict[str, Optional[int]] = {}

    try:
        # PARA DEBUGGING: procesar solo los primeros 3 productos
        productos_test = productos[:3]
        print(f"🧪 MODO DEBUG: procesando solo {len(productos_test)} productos para debugging")
        
        for i, item in enumerate(productos_test, start=1):
            sku = item["SKU"]
            url = item["URL"]
            peso_j = item["PesoJumbo_g"]

            print(f"\n{'='*60}")
            print(f"PRODUCTO {i}/{len(productos_test)} - SKU: {sku}")
            print(f"{'='*60}")

            if not sku:
                print("⚠️ SKU vacío, saltando...")
                dict_sku_precio_kg_jumbo[sku] = None
                continue
                
            if not url:
                print("⚠️ URL vacía, saltando...")
                dict_sku_precio_kg_jumbo[sku] = None
                continue
                
            if not peso_j or float(peso_j) <= 0:
                print(f"⚠️ Peso inválido ({peso_j}g), saltando...")
                dict_sku_precio_kg_jumbo[sku] = None
                continue

            print(f"✅ Datos válidos - Peso: {peso_j}g")
            precio, status = obtener_precio(url, driver)
            
            if precio is None:
                print(f"❌ No se obtuvo precio para SKU {sku} (status: {status})")
                dict_sku_precio_kg_jumbo[sku] = None
            else:
                precio_kg = precio_por_kg(precio, peso_j)
                dict_sku_precio_kg_jumbo[sku] = precio_kg
                print(f"🎉 SKU {sku}: ${precio:,} -> ${precio_kg:,}/kg")
                
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
            
    finally:
        print("🔒 Cerrando navegador...")
        driver.quit()

    print(f"\n📝 Actualizando Google Sheets...")
    
    # 1) Actualizar P-web (columna I), sin pisar valores cuando no hay nuevo
    escribir_pweb(ws_pweb, dict_sku_precio_kg_jumbo)

    # 2) Actualizar Jumbo (histórico) agregando una nueva columna con la fecha
    escribir_jumbo_historico(ws_hist, dict_sku_precio_kg_jumbo, fecha_str)

    # Métricas finales
    total = len(dict_sku_precio_kg_jumbo)
    con_valor = sum(1 for v in dict_sku_precio_kg_jumbo.values() if v is not None)
    sin_valor = total - con_valor
    
    print(f"\n📊 RESUMEN FINAL:")
    print(f"   Total productos: {total}")
    print(f"   Con precio obtenido: {con_valor}")
    print(f"   Sin precio: {sin_valor}")
    if total > 0:
        print(f"   Tasa de éxito: {(con_valor/total*100):.1f}%")
    print("🎉 Proceso completado!")

if __name__ == "__main__":
    main()
