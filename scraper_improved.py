# -*- coding: utf-8 -*-
"""
Scraper mejorado para obtener el precio y el precio por kilo desde Jumbo y
actualizar una planilla de Google Sheets.

CORRECCIONES APLICADAS:
1. Lógica robusta para determinar la columna siguiente en hoja histórica
2. Manejo explícito de errores en batch_update
3. Expansión automática de la hoja si es necesario
4. Validación de escritura exitosa
5. Logs detallados para debugging
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

# Nombres de las hojas
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

SLEEP_MIN = 0.6
SLEEP_MAX = 1.2

# =========================
# Autenticación Google
# =========================

def _get_gspread_client() -> gspread.Client:
    """Autentica contra Google Sheets usando la variable de entorno."""
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
    """Abre el spreadsheet indicado por SHEET_ID."""
    gc = _get_gspread_client()
    return gc.open_by_key(SHEET_ID)


# =========================
# Utilidades de precio
# =========================

PRECIO_REGEXES = [
    re.compile(r"\$\s*([\d\.]+)"),  # $ 7.990
    re.compile(r"CLP\s*([\d\.]+)", re.IGNORECASE),  # CLP 7.990
]

PALABRAS_EXCLUIR = {"prime", "paga", "antes", "suscríbete", "suscribete"}

# Expresión regular para extraer el precio por kg explícito
PRECIO_POR_KG_REGEX = re.compile(
    r"\$?\s*([\d\.,]+)\s*(?:x|/)\s*kg", re.IGNORECASE
)


def normaliza(texto: str) -> str:
    """Normaliza el texto eliminando espacios repetidos y espacios extremos."""
    return " ".join(str(texto).split()).strip()


def extraer_precio(texto: str) -> Optional[int]:
    """Intenta extraer un valor entero desde un string que contenga un precio."""
    t = texto.strip()
    for rx in PRECIO_REGEXES:
        m = rx.search(t)
        if m:
            bruto = m.group(1).replace(".", "").replace(",", "")
            if bruto.isdigit():
                return int(bruto)
    return None


def es_precio_valido(txt: str) -> bool:
    """Determina si un texto es un precio unitario válido."""
    t = txt.lower()
    if not ("$" in t or "clp" in t):
        return False
    if any(p in t for p in PALABRAS_EXCLUIR):
        return False
    if t.startswith(("antes", "normal", "precio normal")):
        return False
    return True


def precio_por_kg(precio: Optional[int], peso_gr: Optional[float]) -> Optional[int]:
    """Calcula el precio por kilo a partir de un precio unitario y un peso en gramos."""
    if precio is None or peso_gr is None:
        return None
    try:
        peso_gr = float(peso_gr)
        if peso_gr <= 0:
            return None
        return round(int(precio) / peso_gr * 1000)
    except Exception:
        return None


def extraer_precio_por_kg(texto: str) -> Optional[int]:
    """Intenta extraer el precio por kg de un texto que contenga «/kg» o «x kg»."""
    m = PRECIO_POR_KG_REGEX.search(texto)
    if m:
        valor = m.group(1).replace(".", "").replace(",", "")
        if valor.isdigit():
            return int(valor)
    return None


# =========================
# Selenium (Chrome headless)
# =========================

def build_browser() -> webdriver.Chrome:
    """Construye una instancia headless de Chrome con un perfil único."""
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

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(90)
    return driver


def encontrar_precios_en_dom(driver: webdriver.Chrome) -> Tuple[Optional[int], Optional[int]]:
    """Busca en el DOM tanto el precio unitario como el precio por kilo."""
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
            continue

    precio_unitario: Optional[int] = None
    precio_kg: Optional[int] = None

    for t in textos:
        # Primero intentamos extraer el precio por kg explícito
        if precio_kg is None:
            pk = extraer_precio_por_kg(t)
            if pk is not None and pk > 0:
                precio_kg = pk
        # Luego el precio unitario
        if precio_unitario is None and es_precio_valido(t):
            p = extraer_precio(t)
            if p is not None and p > 0:
                precio_unitario = p
        # Si tenemos ambos, no hace falta seguir buscando
        if precio_unitario is not None and precio_kg is not None:
            break

    return precio_unitario, precio_kg


def obtener_precios(url: str, driver: webdriver.Chrome, timeout_s: int = 15, retries: int = 2) -> Tuple[Optional[int], Optional[int], str]:
    """Navega a una URL y devuelve los precios encontrados."""
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
            precio_unit, precio_kg = encontrar_precios_en_dom(driver)
            if (precio_unit and precio_unit > 0) or (precio_kg and precio_kg > 0):
                return precio_unit, precio_kg, "ok"
            last_err = "precio_no_encontrado"
        except Exception as e:
            last_err = f"error_navegacion:{type(e).__name__}"
        time.sleep(1.0 + 0.5 * intento)
    return None, None, last_err or "desconocido"


# =========================
# Google Sheets helpers - CORREGIDOS
# =========================

def leer_jumbo_info(sh: gspread.Spreadsheet) -> List[Dict[str, Any]]:
    """Lee todas las filas de la hoja «Jumbo-info» y devuelve una lista de dicts."""
    ws = sh.worksheet(SHEET_JUMBO_INFO)
    values = ws.get_all_values()
    if len(values) < 2:
        return []

    rows: List[Dict[str, Any]] = []
    for r in range(2, len(values) + 1):
        row = values[r - 1]
        while len(row) < 7:
            row.append("")

        sku = str(row[COL_SKU_INFO - 1]).strip()
        url = str(row[COL_URL_INFO - 1]).strip()
        peso_j = row[COL_PESO_JUMBO_INFO - 1]
        precio_gm = row[COL_PRECIO_GM_INFO - 1]
        peso_gm = row[COL_PESO_GM_INFO - 1]

        def to_num(x: Any) -> Optional[float]:
            try:
                return float(str(x).replace(",", "."))
            except Exception:
                return None

        rows.append(
            {
                "row_index": r,
                "SKU": sku,
                "URL": url,
                "PesoJumbo_g": to_num(peso_j),
                "PrecioGM": to_num(precio_gm),
                "PesoGM_g": to_num(peso_gm),
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
    """Actualiza P‑web (columna I = Jumbo Kg) por SKU con manejo de errores mejorado."""
    try:
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
            print(f"  Actualizando {len(updates)} precios en P-web...")
            ws_pweb.batch_update(updates)
            print(f"  ✓ P-web actualizado exitosamente")
        else:
            print("  ⚠ No hay precios nuevos para actualizar en P-web")
    except Exception as e:
        print(f"  ✗ Error al actualizar P-web: {e}")
        raise


def obtener_siguiente_columna_historico(ws_hist: gspread.Worksheet) -> int:
    """
    Obtiene de manera robusta el índice de la siguiente columna disponible.
    Corrige el problema de determinación de columnas.
    """
    try:
        # Obtener la primera fila (encabezados con fechas)
        primera_fila = ws_hist.row_values(1)
        
        # Buscar la última columna con contenido en la primera fila
        ultima_col_con_fecha = len(primera_fila)
        
        # Si hay menos de 3 columnas (A, B), empezamos en C
        if ultima_col_con_fecha < COL_FECHAS_INICIA_EN:
            return COL_FECHAS_INICIA_EN
        
        # La siguiente columna es una después de la última con contenido
        return ultima_col_con_fecha + 1
        
    except Exception as e:
        print(f"  ⚠ Error al determinar columna, usando columna C por defecto: {e}")
        return COL_FECHAS_INICIA_EN


def expandir_hoja_si_necesario(ws: gspread.Worksheet, columnas_necesarias: int, filas_necesarias: int) -> None:
    """
    Expande la hoja si no tiene suficientes columnas o filas.
    """
    try:
        col_count = ws.col_count
        row_count = ws.row_count
        
        necesita_expansion = False
        
        if col_count < columnas_necesarias:
            print(f"  Expandiendo hoja: {col_count} → {columnas_necesarias} columnas")
            ws.add_cols(columnas_necesarias - col_count)
            necesita_expansion = True
            
        if row_count < filas_necesarias:
            print(f"  Expandiendo hoja: {row_count} → {filas_necesarias} filas")
            ws.add_rows(filas_necesarias - row_count)
            necesita_expansion = True
            
        if necesita_expansion:
            time.sleep(1)  # Dar tiempo a Google Sheets para procesar
            
    except Exception as e:
        print(f"  ⚠ Error al expandir hoja: {e}")


def escribir_jumbo_historico(
    ws_hist: gspread.Worksheet, 
    dict_sku_precio_kg: Dict[str, Optional[int]], 
    fecha_str: str
) -> None:
    """
    Versión corregida que maneja correctamente la escritura en hoja histórica.
    """
    try:
        print(f"  Iniciando actualización de hoja histórica con fecha {fecha_str}")
        
        # Paso 1: Obtener mapeo actual de SKUs
        sku_to_row = mapear_sku_a_fila(ws_hist, COL_SKU_HIST)
        print(f"  SKUs existentes en histórico: {len(sku_to_row)}")
        
        # Paso 2: Determinar la columna para la nueva fecha
        new_col_idx = obtener_siguiente_columna_historico(ws_hist)
        print(f"  Nueva columna determinada: {col_idx_to_letter(new_col_idx)} (índice {new_col_idx})")
        
        # Paso 3: Verificar y expandir la hoja si es necesario
        max_row_needed = max(sku_to_row.values()) if sku_to_row else 1
        # Agregar filas para SKUs nuevos
        skus_nuevos = [sku for sku in dict_sku_precio_kg.keys() 
                      if sku and sku not in sku_to_row]
        if skus_nuevos:
            max_row_needed += len(skus_nuevos)
            print(f"  Se agregarán {len(skus_nuevos)} SKUs nuevos")
        
        expandir_hoja_si_necesario(ws_hist, new_col_idx, max_row_needed + 1)
        
        # Paso 4: Escribir encabezado de fecha
        header_a1 = f"{col_idx_to_letter(new_col_idx)}1"
        print(f"  Escribiendo fecha en {header_a1}")
        try:
            ws_hist.update(header_a1, [[fecha_str]], value_input_option='RAW')
            time.sleep(0.5)  # Pequeña pausa para evitar rate limiting
        except Exception as e:
            print(f"  ✗ Error al escribir encabezado: {e}")
            raise
        
        # Paso 5: Agregar SKUs nuevos si existen
        if skus_nuevos:
            print(f"  Agregando {len(skus_nuevos)} SKUs nuevos...")
            to_append = []
            for sku in skus_nuevos:
                # Crear fila con columnas vacías hasta la columna B
                new_row = [""] * (COL_SKU_HIST - 1) + [sku]
                to_append.append(new_row)
            
            if to_append:
                ws_hist.append_rows(to_append, value_input_option="RAW")
                time.sleep(1)  # Esperar para que se procese
                # Actualizar mapeo
                sku_to_row = mapear_sku_a_fila(ws_hist, COL_SKU_HIST)
                print(f"  ✓ SKUs nuevos agregados")
        
        # Paso 6: Escribir valores de precios
        updates = []
        valores_escritos = 0
        valores_vacios = 0
        
        for sku, val in dict_sku_precio_kg.items():
            r = sku_to_row.get(sku)
            if not r or r == 1:
                continue
            
            a1 = f"{col_idx_to_letter(new_col_idx)}{r}"
            if val is None:
                updates.append({"range": a1, "values": [[""]]})
                valores_vacios += 1
            else:
                updates.append({"range": a1, "values": [[val]]})
                valores_escritos += 1
        
        if updates:
            print(f"  Escribiendo {len(updates)} valores ({valores_escritos} con datos, {valores_vacios} vacíos)...")
            # Dividir en lotes para evitar límites de API
            batch_size = 50
            for i in range(0, len(updates), batch_size):
                batch = updates[i:i+batch_size]
                ws_hist.batch_update(batch, value_input_option='RAW')
                if i + batch_size < len(updates):
                    time.sleep(0.5)  # Pausa entre lotes
            print(f"  ✓ Histórico actualizado exitosamente")
        else:
            print("  ⚠ No hay valores para escribir en histórico")
            
    except Exception as e:
        print(f"  ✗ Error crítico al actualizar histórico: {e}")
        raise


def col_idx_to_letter(idx: int) -> str:
    """Convierte un índice de columna (1‑based) a la letra utilizada en A1."""
    letters = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


# =========================
# Flujo principal
# =========================

def main() -> None:
    """Flujo principal con manejo de errores mejorado y operaciones independientes."""
    
    # Fecha local America/Santiago
    tz_scl = tz.gettz("America/Santiago")
    fecha_str = datetime.now(tz_scl).strftime("%d-%m-%Y")
    print(f"\n{'='*60}")
    print(f"INICIANDO SCRAPER - {fecha_str}")
    print(f"{'='*60}\n")

    try:
        # Abrir spreadsheet
        print("1. Conectando con Google Sheets...")
        sh = open_sheet()
        ws_pweb = sh.worksheet(SHEET_PWEB)
        ws_hist = sh.worksheet(SHEET_JUMBO_HIST)
        print("   ✓ Conexión establecida\n")

        # Leer productos
        print("2. Leyendo productos de Jumbo-info...")
        productos = leer_jumbo_info(sh)
        if not productos:
            print("   ✗ No hay filas en Jumbo-info.")
            return
        print(f"   ✓ {len(productos)} productos encontrados\n")

        # Scraping
        print("3. Iniciando scraping de precios...")
        driver = build_browser()
        dict_sku_precio_kg_jumbo: Dict[str, Optional[int]] = {}
        
        errores_scraping = []
        exitosos = 0

        try:
            for i, item in enumerate(productos, start=1):
                sku = item.get("SKU", "")
                url = item.get("URL", "")
                peso_j = item.get("PesoJumbo_g")

                if not sku:
                    continue

                if not url:
                    dict_sku_precio_kg_jumbo[sku] = None
                    errores_scraping.append(f"SKU {sku}: Sin URL")
                    continue

                try:
                    precio_unit, precio_kg_encontrado, status = obtener_precios(url, driver)
                    
                    if precio_kg_encontrado is not None:
                        dict_sku_precio_kg_jumbo[sku] = precio_kg_encontrado
                        exitosos += 1
                    elif precio_unit is not None and peso_j:
                        valor = precio_por_kg(precio_unit, peso_j)
                        dict_sku_precio_kg_jumbo[sku] = valor
                        if valor:
                            exitosos += 1
                    else:
                        dict_sku_precio_kg_jumbo[sku] = None
                        errores_scraping.append(f"SKU {sku}: {status}")
                        
                except Exception as e:
                    dict_sku_precio_kg_jumbo[sku] = None
                    errores_scraping.append(f"SKU {sku}: {str(e)}")

                if i % 10 == 0:
                    print(f"   Procesados {i}/{len(productos)}")
                    
                time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
                
        finally:
            driver.quit()
            print(f"   ✓ Scraping completado: {exitosos}/{len(productos)} exitosos\n")
            
            if errores_scraping and len(errores_scraping) <= 5:
                print("   Errores de scraping:")
                for err in errores_scraping[:5]:
                    print(f"     - {err}")
                print()

        # Actualización de hojas - OPERACIONES INDEPENDIENTES
        print("4. Actualizando hojas de Google Sheets...")
        
        # Actualizar P-web (independiente del histórico)
        actualizado_pweb = False
        try:
            print("\n   4.1 Actualizando P-web...")
            escribir_pweb(ws_pweb, dict_sku_precio_kg_jumbo)
            actualizado_pweb = True
        except Exception as e:
            print(f"   ✗ Error al actualizar P-web: {e}")
            # No hacer raise aquí para intentar actualizar histórico

        # Actualizar Histórico (independiente de P-web)
        actualizado_historico = False
        try:
            print("\n   4.2 Actualizando Jumbo histórico...")
            escribir_jumbo_historico(ws_hist, dict_sku_precio_kg_jumbo, fecha_str)
            actualizado_historico = True
        except Exception as e:
            print(f"   ✗ Error al actualizar histórico: {e}")

        # Resumen final
        print(f"\n{'='*60}")
        print("RESUMEN DE EJECUCIÓN")
        print(f"{'='*60}")
        
        total = len(dict_sku_precio_kg_jumbo)
        con_valor = sum(1 for v in dict_sku_precio_kg_jumbo.values() if v is not None)
        sin_valor = total - con_valor
        
        print(f"Productos procesados: {total}")
        print(f"  - Con precio: {con_valor}")
        print(f"  - Sin precio: {sin_valor}")
        print(f"\nActualizaciones:")
        print(f"  - P-web: {'✓ Exitosa' if actualizado_pweb else '✗ Fallida'}")
        print(f"  - Histórico: {'✓ Exitosa' if actualizado_historico else '✗ Fallida'}")
        
        # Si alguna actualización falló, lanzar error para que GitHub lo detecte
        if not actualizado_pweb or not actualizado_historico:
            error_msg = "Algunas actualizaciones fallaron: "
            fallidas = []
            if not actualizado_pweb:
                fallidas.append("P-web")
            if not actualizado_historico:
                fallidas.append("Histórico")
            error_msg += ", ".join(fallidas)
            print(f"\n⚠ {error_msg}")
            raise RuntimeError(error_msg)
            
        print(f"\n✓ PROCESO COMPLETADO EXITOSAMENTE")
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"\n✗ ERROR CRÍTICO: {e}")
        print(f"{'='*60}\n")
        raise


if __name__ == "__main__":
    main()
