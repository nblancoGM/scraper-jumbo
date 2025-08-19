# -*- coding: utf-8 -*-
"""
Scraper para obtener el precio por kilo desde Jumbo Chile y
actualizar únicamente la columna I (Jumbo Kg) de la hoja P-web.

Versión mejorada con:
- selenium-stealth para evitar la detección de bots.
- Estrategias de espera robustas para contenido dinámico.
- Selectores CSS actualizados para la nueva estructura de Jumbo.
- Manejo de pop-ups de ubicación.
"""

from __future__ import annotations

import os
import re
import time
import json
import random
from typing import Optional, Tuple, List, Dict, Any

import gspread  # type: ignore
from google.oauth2.service_account import Credentials  # type: ignore

from selenium import webdriver  # type: ignore
from selenium.webdriver.common.by import By  # type: ignore
from selenium.webdriver.chrome.options import Options  # type: ignore
from selenium.webdriver.support.ui import WebDriverWait  # type: ignore
from selenium.webdriver.support import expected_conditions as EC  # type: ignore
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException  # type: ignore
from selenium_stealth import stealth # type: ignore

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

SLEEP_MIN = 3.0
SLEEP_MAX = 6.0

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
    """Abre el spreadsheet indicado por ``SHEET_ID``."""
    gc = _get_gspread_client()
    return gc.open_by_key(SHEET_ID)


# =========================
# Utilidades de precio
# =========================

def normaliza(texto: str) -> str:
    """Normaliza el texto eliminando espacios repetidos y espacios extremos."""
    return " ".join(str(texto).split()).strip()


def extraer_precio(texto: str) -> Optional[int]:
    """Extrae un valor numérico de un texto que representa un precio."""
    if not texto:
        return None
    try:
        # Limpia el texto de símbolos y puntos, luego convierte a entero
        valor_str = re.sub(r'[^\d]', '', texto)
        if valor_str.isdigit():
            return int(valor_str)
    except (ValueError, TypeError):
        return None
    return None


def precio_por_kg(precio: Optional[int], peso_gr: Optional[float]) -> Optional[int]:
    """Calcula el precio por kilo a partir de un precio unitario y un peso en gramos."""
    if precio is None or peso_gr is None:
        return None
    try:
        peso_gr_float = float(peso_gr)
        if peso_gr_float <= 0:
            return None
        # Calcula el precio por kilo y lo redondea al entero más cercano
        return round(precio / peso_gr_float * 1000)
    except (ValueError, TypeError, ZeroDivisionError):
        return None


# =========================
# Selenium mejorado para Jumbo Chile
# =========================

def build_browser() -> webdriver.Chrome:
    """Construye una instancia de Chrome con selenium-stealth para evitar la detección."""
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    # User agent común
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    )

    chrome_bin = os.environ.get("CHROME_BIN", "").strip()
    if chrome_bin and os.path.exists(chrome_bin):
        options.binary_location = chrome_bin

    driver = webdriver.Chrome(options=options)
    
    # Configuración de Stealth
    stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
            )
            
    driver.set_page_load_timeout(120)
    return driver


def esperar_y_cerrar_popup_ubicacion(driver: webdriver.Chrome, timeout: int = 10):
    """Espera si aparece el pop-up de ubicación y lo cierra."""
    try:
        # Espera a que el botón de cierre del modal sea clickeable
        close_button_selector = "button.absolute.top-4.right-4"
        close_button = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, close_button_selector))
        )
        print("ℹ️ Detectado pop-up de ubicación. Intentando cerrar...")
        # Cierra el modal con un clic de JavaScript para más fiabilidad
        driver.execute_script("arguments[0].click();", close_button)
        print("✓ Pop-up de ubicación cerrado.")
        time.sleep(2)  # Pequeña pausa después de cerrar
    except TimeoutException:
        print("✓ No se encontró el pop-up de ubicación (o ya estaba cerrado).")
    except Exception as e:
        print(f"⚠️ Error al intentar cerrar el pop-up de ubicación: {e}")


def encontrar_precios_jumbo(driver: webdriver.Chrome) -> Tuple[Optional[int], Optional[int]]:
    """
    Busca precios en la estructura actual de Jumbo Chile.
    Prioriza encontrar el precio por kg, y si no, el precio unitario.
    """
    precio_unitario: Optional[int] = None
    precio_kg: Optional[int] = None

    try:
        # Estrategia 1: Buscar el precio por kilo explícito
        # El precio por kilo suele estar en un div con un texto como "$11.990 x kg"
        # Usamos XPath para encontrar un div que contenga 'x kg'
        elementos_kg = driver.find_elements(By.XPATH, "//*[contains(text(), 'x kg')]")
        for elem in elementos_kg:
            texto = elem.text
            if texto and "x kg" in texto.lower():
                # Extraemos solo el número del texto
                precio_kg = extraer_precio(texto)
                if precio_kg:
                    print(f"✓ Precio/kg encontrado directamente: ${precio_kg} (texto: '{texto}')")
                    break # Nos quedamos con el primero que encontremos

        # Estrategia 2: Si no hay precio por kg, buscar el precio unitario
        if not precio_kg:
            # El precio principal suele estar en un span con una clase específica
            # Buscamos por el selector CSS que corresponde al precio principal
            try:
                precio_elem = driver.find_element(By.CSS_SELECTOR, "span.price-best")
                texto_precio = precio_elem.text
                precio_unitario = extraer_precio(texto_precio)
                if precio_unitario:
                    print(f"✓ Precio unitario encontrado: ${precio_unitario} (texto: '{texto_precio}')")
            except NoSuchElementException:
                print("⚠️ No se encontró el elemento de precio unitario con el selector 'span.price-best'.")

    except Exception as e:
        print(f"❌ Error buscando precios en la página: {e}")

    return precio_unitario, precio_kg


def obtener_precios_jumbo(url: str, driver: webdriver.Chrome, timeout_s: int = 60, retries: int = 3) -> Tuple[Optional[int], Optional[int], str]:
    """Navega a una URL de Jumbo y extrae precios, con reintentos y lógica mejorada."""
    print(f"Navegando a: {url}")
    last_err = ""

    for intento in range(1, retries + 1):
        try:
            print(f"Intento {intento}/{retries}")
            driver.get(url)

            # 1. Intentar cerrar el pop-up de ubicación que bloquea la vista
            esperar_y_cerrar_popup_ubicacion(driver)

            # 2. Esperar a que el contenedor principal del producto sea visible
            # Este es un indicador mucho más fiable de que la página ha cargado
            print("Esperando a que el contenido principal del producto cargue...")
            WebDriverWait(driver, timeout_s).until(
                EC.visibility_of_element_located((By.ID, "main-content-product-detail"))
            )
            print("✓ Contenido principal cargado.")
            
            # Pequeña espera adicional para asegurar que todos los scripts se ejecuten
            time.sleep(random.uniform(2.0, 4.0))

            # 3. Buscar precios
            precio_unit, precio_kg = encontrar_precios_jumbo(driver)

            if precio_unit is not None or precio_kg is not None:
                return precio_unit, precio_kg, "ok"
            else:
                last_err = "precio_no_encontrado"
                print(f"⚠️ No se encontraron precios en el intento {intento}.")
                # Guardar un screenshot para depuración
                screenshot_path = f"debug_no_precio_sku_{url.split('/')[-2]}_intento_{intento}.png"
                driver.save_screenshot(screenshot_path)
                print(f"📸 Screenshot de depuración guardado en: {screenshot_path}")

        except TimeoutException:
            last_err = "timeout_navegacion"
            print(f"⚠️ Timeout esperando que la página cargue (intento {intento}).")
            screenshot_path = f"debug_timeout_sku_{url.split('/')[-2]}_intento_{intento}.png"
            driver.save_screenshot(screenshot_path)
            print(f"📸 Screenshot de depuración guardado en: {screenshot_path}")
        except WebDriverException as e:
            last_err = f"webdriver_error:{type(e).__name__}"
            print(f"⚠️ Error de WebDriver (intento {intento}): {e}")
        except Exception as e:
            last_err = f"error_general:{type(e).__name__}"
            print(f"⚠️ Error inesperado (intento {intento}): {e}")

        if intento < retries:
            wait_time = 3 * intento
            print(f"Esperando {wait_time}s antes del siguiente intento...")
            time.sleep(wait_time)

    return None, None, last_err or "fallos_multiples"


# =========================
# Google Sheets helpers
# =========================

def leer_jumbo_info(sh: gspread.Spreadsheet) -> List[Dict[str, Any]]:
    """Lee todas las filas de la hoja «Jumbo-info» y devuelve una lista de dicts."""
    ws = sh.worksheet(SHEET_JUMBO_INFO)
    values = ws.get_all_values()
    if len(values) < 2:
        return []

    rows: List[Dict[str, Any]] = []
    # Empezamos desde la segunda fila (índice 1) para saltar el encabezado
    for i, row in enumerate(values[1:], start=2):
        # Asegurarse de que haya suficientes columnas para evitar IndexError
        sku = row[COL_SKU_INFO - 1].strip() if len(row) >= COL_SKU_INFO else ""
        url = row[COL_URL_INFO - 1].strip() if len(row) >= COL_URL_INFO else ""
        peso_j_str = row[COL_PESO_JUMBO_INFO - 1] if len(row) >= COL_PESO_JUMBO_INFO else ""

        def to_num(x: Any) -> Optional[float]:
            try:
                # Reemplaza la coma decimal por un punto
                return float(str(x).replace(",", "."))
            except (ValueError, TypeError):
                return None

        rows.append(
            {
                "row_index": i,
                "SKU": sku,
                "URL": url,
                "PesoJumbo_g": to_num(peso_j_str),
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
    """Actualiza P‑web (columna I = Jumbo Kg) por SKU usando batch_update."""
    sku_to_row = mapear_sku_a_fila(ws_pweb, COL_SKU_PWEB)
    updates: List[Dict[str, Any]] = []
    
    actualizados = 0
    for sku, nuevo_precio in dict_sku_precio_kg.items():
        row_idx = sku_to_row.get(sku)
        if not row_idx or row_idx == 1:
            continue
        # Solo actualizamos si el nuevo precio es un número válido
        if isinstance(nuevo_precio, (int, float)):
            a1_notation = f"I{row_idx}"
            updates.append({"range": a1_notation, "values": [[nuevo_precio]]})
            actualizados += 1
    
    if updates:
        print(f"Actualizando {len(updates)} celdas en P-web...")
        try:
            ws_pweb.batch_update(updates)
            print(f"✅ {actualizados} valores actualizados en P-web columna I")
        except Exception as e:
            print(f"❌ Error actualizando P-web: {e}")
    else:
        print("⚠️ No hay valores válidos para actualizar en P-web.")


# =========================
# Flujo principal
# =========================

def main() -> None:
    print("🚀 Iniciando scraper de Jumbo Chile - v2 (con Stealth y selectores actualizados)")
    
    sh = open_sheet()
    ws_pweb = sh.worksheet(SHEET_PWEB)

    productos = leer_jumbo_info(sh)
    if not productos:
        print("❌ No hay filas válidas en la hoja Jumbo-info.")
        return

    print(f"📊 Productos a procesar: {len(productos)}")

    driver = build_browser()
    dict_sku_precio_kg_jumbo: Dict[str, Optional[int]] = {}
    
    procesados_exitosos = 0
    fallos = 0

    try:
        for i, item in enumerate(productos, start=1):
            sku = item.get("SKU")
            url = item.get("URL")
            peso_j = item.get("PesoJumbo_g")

            print(f"\n{'='*60}")
            print(f"🔍 [{i}/{len(productos)}] Procesando SKU: {sku}")

            if not sku or not url or not url.startswith("http"):
                dict_sku_precio_kg_jumbo[sku] = None
                fallos += 1
                print(f"⚠️ SKU {sku}: Datos incompletos o URL inválida. Saltando.")
                continue

            precio_unit, precio_kg_encontrado, status = obtener_precios_jumbo(url, driver)
            
            precio_final_kg = None
            if precio_kg_encontrado is not None:
                # Prioridad 1: Precio por kg encontrado directamente
                precio_final_kg = precio_kg_encontrado
                procesados_exitosos += 1
                print(f"✅ SKU {sku}: Precio/kg directo = ${precio_final_kg}")
                
            elif precio_unit is not None and peso_j is not None:
                # Prioridad 2: Calcular precio por kg
                precio_final_kg = precio_por_kg(precio_unit, peso_j)
                if precio_final_kg:
                    procesados_exitosos += 1
                    print(f"✅ SKU {sku}: Precio/kg calculado = ${precio_final_kg} (de ${precio_unit} / {peso_j}g)")
                else:
                    fallos += 1
                    print(f"❌ SKU {sku}: Error en cálculo de precio/kg (precio_unit: {precio_unit}, peso: {peso_j})")
            else:
                fallos += 1
                print(f"❌ SKU {sku}: No se pudo obtener ningún precio ({status})")

            dict_sku_precio_kg_jumbo[sku] = precio_final_kg

            if i % 10 == 0:
                print(f"\n📈 Progreso: {i}/{len(productos)} | Exitosos: {procesados_exitosos} | Fallos: {fallos}")
            
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
            
    except KeyboardInterrupt:
        print("\n⚠️ Proceso interrumpido por el usuario.")
    except Exception as e:
        print(f"\n❌ Error fatal inesperado: {e}")
    finally:
        print("Cerrando navegador...")
        driver.quit()

    print(f"\n{'='*60}")
    print("📝 Actualizando hoja P-web con los resultados obtenidos...")
    escribir_pweb(ws_pweb, dict_sku_precio_kg_jumbo)

    total = len(dict_sku_precio_kg_jumbo)
    if total > 0:
        con_valor = sum(1 for v in dict_sku_precio_kg_jumbo.values() if v is not None)
        sin_valor = total - con_valor
        tasa_exito = (con_valor / total * 100) if total > 0 else 0
        
        print("\n📊 RESUMEN FINAL:")
        print(f"   Total de SKUs procesados: {total}")
        print(f"   ✅ Con precio/kg obtenido: {con_valor}")
        print(f"   ❌ Sin precio/kg obtenido: {sin_valor}")
        print(f"   📈 Tasa de éxito: {tasa_exito:.1f}%")
    else:
        print("\n📊 No se procesaron productos.")


if __name__ == "__main__":
    main()
