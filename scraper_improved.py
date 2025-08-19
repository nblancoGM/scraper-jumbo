# -*- coding: utf-8 -*-
"""
Scraper para obtener el precio por kilo desde Jumbo Chile y
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
from selenium.common.exceptions import TimeoutException, WebDriverException  # type: ignore

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

SLEEP_MIN = 1.5  # Aumentado para dar más tiempo
SLEEP_MAX = 3.0

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


def extraer_precio_mejorado(texto: str) -> Optional[int]:
    """Extrae precio con múltiples patrones para Jumbo Chile."""
    if not texto:
        return None
    
    # Patrones más específicos para Jumbo Chile
    patrones = [
        r'\$\s*([\d{1,3}\.]*\d{1,3})\s*(?:CLP)?',  # $7.990, $7990, etc
        r'([\d{1,3}\.]*\d{1,3})\s*CLP',            # 7.990 CLP
        r'precio[:\s]*\$?\s*([\d{1,3}\.]*\d{1,3})', # Precio: $7990
        r'valor[:\s]*\$?\s*([\d{1,3}\.]*\d{1,3})',  # Valor: $7990
        r'\$\s*([\d,]+)',                           # Con comas
    ]
    
    texto_clean = texto.lower().strip()
    
    # Palabras que invalidan el precio
    palabras_excluir = {
        'antes', 'normal', 'precio normal', 'descuento', 'ahorro',
        'prime', 'suscríbete', 'membresía', 'gratis', 'envío',
        'despacho', 'retiro', 'tienda', 'stock'
    }
    
    if any(palabra in texto_clean for palabra in palabras_excluir):
        return None
    
    for patron in patrones:
        matches = re.finditer(patron, texto, re.IGNORECASE)
        for match in matches:
            try:
                valor_str = match.group(1).replace('.', '').replace(',', '').strip()
                if valor_str.isdigit():
                    valor = int(valor_str)
                    # Filtrar valores irreales (muy bajos o muy altos)
                    if 100 <= valor <= 1000000:
                        return valor
            except (ValueError, IndexError):
                continue
    
    return None


def extraer_precio_por_kg_mejorado(texto: str) -> Optional[int]:
    """Extrae precio por kg con patrones específicos para Chile."""
    if not texto:
        return None
    
    # Patrones para precio por kg específicos de Chile
    patrones_kg = [
        r'\$\s*([\d{1,3}\.]*\d{1,3})\s*(?:/|por|x)\s*k?g',          # $7.990/kg, $7990 por kg
        r'([\d{1,3}\.]*\d{1,3})\s*/\s*k?g',                         # 7990/kg
        r'k?g\s*\$?\s*([\d{1,3}\.]*\d{1,3})',                       # kg $7990
        r'kilo[:\s]*\$?\s*([\d{1,3}\.]*\d{1,3})',                   # kilo: $7990
        r'precio\s*k?g[:\s]*\$?\s*([\d{1,3}\.]*\d{1,3})',          # precio kg: $7990
    ]
    
    for patron in patrones_kg:
        matches = re.finditer(patron, texto, re.IGNORECASE)
        for match in matches:
            try:
                valor_str = match.group(1).replace('.', '').replace(',', '').strip()
                if valor_str.isdigit():
                    valor = int(valor_str)
                    if 500 <= valor <= 500000:  # Rango razonable para precio/kg
                        return valor
            except (ValueError, IndexError):
                continue
    
    return None


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


# =========================
# Selenium mejorado para Jumbo Chile
# =========================

def build_browser() -> webdriver.Chrome:
    """Construye una instancia headless de Chrome optimizada para Jumbo Chile."""
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
    options.add_argument("--disable-features=TranslateUI,VizDisplayCompositor")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-web-security")
    options.add_argument("--allow-running-insecure-content")
    
    # User agent más reciente
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    )

    # Crear perfil único
    profile_dir = os.path.join(tempfile.gettempdir(), f"chrome-profile-{uuid.uuid4()}")
    os.makedirs(profile_dir, exist_ok=True)
    options.add_argument(f"--user-data-dir={profile_dir}")

    chrome_bin = os.environ.get("CHROME_BIN", "").strip()
    if chrome_bin and os.path.exists(chrome_bin):
        options.binary_location = chrome_bin

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(120)  # Aumentado a 2 minutos
    return driver


def esperar_contenido_dinamico(driver: webdriver.Chrome, timeout: int = 30) -> bool:
    """Espera a que el contenido dinámico de Jumbo se cargue completamente."""
    try:
        # Esperar múltiples indicadores de que la página se cargó
        conditions = [
            # Esperar precios
            EC.presence_of_element_located((By.XPATH, "//*[contains(text(), '$') or contains(text(), 'CLP')]")),
            # Esperar contenido del producto
            EC.presence_of_element_located((By.XPATH, "//*[contains(@class, 'price') or contains(@class, 'precio')]")),
            # Esperar elementos con data attributes de precio
            EC.presence_of_element_located((By.XPATH, "//*[contains(@data-testid, 'price') or contains(@data-qa, 'price')]")),
        ]
        
        # Intentar cada condición
        for condition in conditions:
            try:
                WebDriverWait(driver, timeout // len(conditions)).until(condition)
                return True
            except TimeoutException:
                continue
                
        # Si no funcionó ninguna condición específica, esperar JavaScript
        WebDriverWait(driver, 5).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        
        # Tiempo adicional para elementos dinámicos
        time.sleep(3)
        return True
        
    except Exception:
        return False


def encontrar_precios_jumbo(driver: webdriver.Chrome) -> Tuple[Optional[int], Optional[int]]:
    """Busca precios específicamente en la estructura de Jumbo Chile."""
    
    # Selectores específicos para Jumbo Chile
    selectores_precio = [
        # Selectores de precio más específicos
        "[data-testid*='price']",
        "[data-qa*='price']", 
        "[class*='price']",
        "[class*='precio']",
        "[class*='valor']",
        "[data-price]",
        ".vtex-product-price",
        ".vtex-store-components",
        ".product-price",
        ".selling-price",
        ".best-price",
        ".current-price",
        ".price-current",
        ".price-value",
        # Selectores más generales
        "span[class*='price']",
        "div[class*='price']",
        "p[class*='price']",
        "span[class*='precio']",
        "div[class*='precio']",
        # Elementos que contengan símbolo de peso
        "span:contains('$')",
        "div:contains('$')",
        "p:contains('$')",
    ]
    
    textos_encontrados = []
    
    # Buscar con JavaScript también
    try:
        js_prices = driver.execute_script("""
            var prices = [];
            var elements = document.querySelectorAll('*');
            for (var i = 0; i < elements.length; i++) {
                var text = elements[i].textContent || elements[i].innerText || '';
                if (text.includes('$') || text.includes('CLP') || text.includes('precio')) {
                    prices.push(text.trim());
                }
            }
            return prices.slice(0, 50); // Limitar resultados
        """)
        textos_encontrados.extend(js_prices)
    except Exception as e:
        print(f"Error ejecutando JS: {e}")
    
    # Buscar con selectores CSS
    for selector in selectores_precio:
        try:
            elementos = driver.find_elements(By.CSS_SELECTOR, selector)
            for elem in elementos[:10]:  # Limitar por selector
                try:
                    texto = elem.text.strip()
                    if texto and ('$' in texto or 'CLP' in texto.upper()):
                        textos_encontrados.append(texto)
                    
                    # También revisar atributos
                    for attr in ['data-price', 'data-value', 'title', 'aria-label']:
                        attr_value = elem.get_attribute(attr)
                        if attr_value and ('$' in attr_value or 'CLP' in attr_value.upper()):
                            textos_encontrados.append(attr_value)
                except Exception:
                    continue
        except Exception:
            continue
    
    # También buscar en el texto completo de la página
    try:
        body_text = driver.find_element(By.TAG_NAME, "body").text
        # Buscar patrones de precio en todo el texto
        price_patterns = re.findall(r'\$[\s\d.,]+|\d+[\s.,]*CLP', body_text, re.IGNORECASE)
        textos_encontrados.extend(price_patterns[:20])
    except Exception:
        pass
    
    precio_unitario: Optional[int] = None
    precio_kg: Optional[int] = None
    
    print(f"Textos encontrados para análisis: {len(textos_encontrados)}")
    
    # Analizar textos encontrados
    for texto in textos_encontrados:
        if not texto or len(texto.strip()) == 0:
            continue
            
        texto_clean = normaliza(texto)
        print(f"Analizando: '{texto_clean[:100]}'")
        
        # Primero buscar precio por kg
        if precio_kg is None:
            pk = extraer_precio_por_kg_mejorado(texto_clean)
            if pk is not None:
                precio_kg = pk
                print(f"Precio/kg encontrado: ${pk}")
        
        # Luego buscar precio unitario
        if precio_unitario is None:
            pu = extraer_precio_mejorado(texto_clean)
            if pu is not None:
                precio_unitario = pu
                print(f"Precio unitario encontrado: ${pu}")
        
        # Si tenemos ambos, podemos parar
        if precio_unitario is not None and precio_kg is not None:
            break
    
    return precio_unitario, precio_kg


def obtener_precios_jumbo(url: str, driver: webdriver.Chrome, timeout_s: int = 45, retries: int = 3) -> Tuple[Optional[int], Optional[int], str]:
    """Navega a una URL de Jumbo Chile y extrae precios con estrategia mejorada."""
    
    print(f"Navegando a: {url}")
    last_err = ""
    
    for intento in range(1, retries + 1):
        try:
            # Navegar a la URL
            driver.get(url)
            
            # Esperar que la página cargue completamente
            if not esperar_contenido_dinamico(driver, timeout_s):
                print(f"Timeout esperando contenido dinámico (intento {intento})")
                last_err = "timeout_contenido_dinamico"
                continue
            
            # Scroll para activar lazy loading
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(2)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)
            
            # Buscar precios
            precio_unit, precio_kg = encontrar_precios_jumbo(driver)
            
            if precio_unit is not None or precio_kg is not None:
                return precio_unit, precio_kg, "ok"
            else:
                last_err = "precio_no_encontrado"
                print(f"No se encontraron precios (intento {intento})")
                
        except TimeoutException:
            last_err = "timeout_navegacion"
            print(f"Timeout de navegación (intento {intento})")
        except WebDriverException as e:
            last_err = f"webdriver_error:{type(e).__name__}"
            print(f"Error WebDriver (intento {intento}): {e}")
        except Exception as e:
            last_err = f"error_general:{type(e).__name__}"
            print(f"Error general (intento {intento}): {e}")
        
        # Espera progresivamente más larga entre intentos
        if intento < retries:
            wait_time = 2 * intento
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
    """Actualiza P‑web (columna I = Jumbo Kg) por SKU usando batch_update."""
    sku_to_row = mapear_sku_a_fila(ws_pweb, COL_SKU_PWEB)
    updates: List[Dict[str, Any]] = []
    
    actualizados = 0
    for sku, nuevo in dict_sku_precio_kg.items():
        row = sku_to_row.get(sku)
        if not row or row == 1:
            continue
        if nuevo is None or nuevo == "":
            continue
        
        a1 = f"I{row}"
        updates.append({"range": a1, "values": [[nuevo]]})
        actualizados += 1
    
    if updates:
        print(f"Actualizando {len(updates)} celdas en P-web...")
        try:
            ws_pweb.batch_update(updates)
            print(f"✅ {actualizados} valores actualizados en P-web columna I")
        except Exception as e:
            print(f"❌ Error actualizando P-web: {e}")
    else:
        print("⚠️ No hay valores para actualizar en P-web")


# =========================
# Flujo principal
# =========================

def main() -> None:
    print("🚀 Iniciando scraper de Jumbo Chile - Solo P-web")
    
    sh = open_sheet()
    ws_pweb = sh.worksheet(SHEET_PWEB)

    productos = leer_jumbo_info(sh)
    if not productos:
        print("❌ No hay filas en Jumbo-info.")
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

            print(f"\n🔍 [{i}/{len(productos)}] Procesando SKU: {sku}")

            if not sku or not url:
                dict_sku_precio_kg_jumbo[sku] = None
                fallos += 1
                print(f"⚠️ SKU {sku}: Datos incompletos")
                continue

            precio_unit, precio_kg_encontrado, status = obtener_precios_jumbo(url, driver)
            
            if precio_kg_encontrado is not None:
                # Precio por kg encontrado directamente
                dict_sku_precio_kg_jumbo[sku] = precio_kg_encontrado
                procesados_exitosos += 1
                print(f"✅ SKU {sku}: Precio/kg directo = ${precio_kg_encontrado}")
                
            elif precio_unit is not None and peso_j is not None:
                # Calcular precio por kg usando peso
                valor = precio_por_kg(precio_unit, peso_j)
                dict_sku_precio_kg_jumbo[sku] = valor
                if valor:
                    procesados_exitosos += 1
                    print(f"✅ SKU {sku}: Precio/kg calculado = ${valor} (${precio_unit}/{peso_j}g)")
                else:
                    fallos += 1
                    print(f"❌ SKU {sku}: Error en cálculo precio/kg")
            else:
                dict_sku_precio_kg_jumbo[sku] = None
                fallos += 1
                print(f"❌ SKU {sku}: No se pudo obtener precio ({status})")

            # Progreso cada 10 productos
            if i % 10 == 0:
                print(f"📈 Progreso: {i}/{len(productos)} | Exitosos: {procesados_exitosos} | Fallos: {fallos}")
            
            # Sleep entre requests
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
            
    except KeyboardInterrupt:
        print("\n⚠️ Proceso interrumpido por el usuario")
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")
    finally:
        driver.quit()

    # Actualizar P-web
    print(f"\n📝 Actualizando hoja P-web...")
    escribir_pweb(ws_pweb, dict_sku_precio_kg_jumbo)

    # Métricas finales
    total = len(dict_sku_precio_kg_jumbo)
    con_valor = sum(1 for v in dict_sku_precio_kg_jumbo.values() if v is not None)
    sin_valor = total - con_valor
    
    print(f"\n📊 RESUMEN FINAL:")
    print(f"   Total procesados: {total}")
    print(f"   ✅ Con valor: {con_valor}")
    print(f"   ❌ Sin valor: {sin_valor}")
    print(f"   📈 Tasa de éxito: {(con_valor/total*100):.1f}%" if total > 0 else "   📈 Tasa de éxito: 0%")


if __name__ == "__main__":
    main()
