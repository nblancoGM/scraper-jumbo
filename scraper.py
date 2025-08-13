# -*- coding: utf-8 -*-
"""
Scraper mejorado para Jumbo.cl - Maneja JavaScript y estructura moderna
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import re
import json
import tempfile
import uuid
import os

def build_browser():
    """Configuración mejorada para manejar sitios con JavaScript"""
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--window-size=1920,1080")
    
    # User agent más realista
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    
    # Perfil único
    profile_dir = os.path.join(tempfile.gettempdir(), f"chrome-profile-{uuid.uuid4()}")
    os.makedirs(profile_dir, exist_ok=True)
    options.add_argument(f"--user-data-dir={profile_dir}")
    
    chrome_bin = os.environ.get("CHROME_BIN", "").strip()
    if chrome_bin and os.path.exists(chrome_bin):
        options.binary_location = chrome_bin
    
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(120)
    
    # Ejecutar script para ocultar que es automatizado
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def extraer_precio_texto(texto):
    """Función mejorada para extraer precios"""
    # Limpiar texto
    texto = texto.replace('\n', ' ').replace('\t', ' ').strip()
    
    # Patrón más flexible para precios chilenos
    patrones = [
        r'\$\s*([\d\.]+)',  # $1.234
        r'\$\s*([\d,]+)',   # $1,234
        r'(\d{1,3}(?:\.\d{3})*)\s*pesos',  # 1.234 pesos
        r'(\d{1,3}(?:,\d{3})*)\s*pesos',   # 1,234 pesos
        r'CLP\s*([\d\.]+)',  # CLP 1234
        r'(\d{1,6})\s*$'     # Solo números al final
    ]
    
    for patron in patrones:
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            precio_str = match.group(1).replace('.', '').replace(',', '')
            try:
                precio = int(precio_str)
                if 100 <= precio <= 1000000:  # Rango razonable para productos
                    return precio
            except ValueError:
                continue
    
    return None

def obtener_precio_mejorado(url: str, driver: webdriver.Chrome, timeout_s: int = 30) -> tuple:
    """Estrategia mejorada para obtener precios de Jumbo.cl"""
    print(f"🌐 Procesando URL: {url}")
    
    try:
        driver.get(url)
        print("   📄 Página cargada, esperando JavaScript...")
        
        # Esperar más tiempo para JavaScript
        time.sleep(8)
        
        # Estrategias múltiples para encontrar el precio
        estrategias = [
            # Estrategia 1: Selectores específicos de precio
            {
                'name': 'Selectores específicos',
                'selectors': [
                    '[data-testid*="price"]',
                    '[class*="price"]',
                    '[class*="Price"]',
                    '[id*="price"]',
                    '.vtex-product-price',
                    '.vtex-store-components',
                    '[class*="currency"]'
                ]
            },
            # Estrategia 2: Texto que contiene $
            {
                'name': 'Elementos con símbolo $',
                'method': 'dollar_search'
            },
            # Estrategia 3: Números grandes (precios)
            {
                'name': 'Búsqueda por patrones de precio',
                'method': 'pattern_search'
            }
        ]
        
        for estrategia in estrategias:
            print(f"   🔍 Probando estrategia: {estrategia['name']}")
            
            if estrategia.get('method') == 'dollar_search':
                # Buscar todos los elementos que contengan $
                try:
                    elementos = driver.find_elements(By.XPATH, "//*[contains(text(), '$')]")
                    print(f"      Encontrados {len(elementos)} elementos con '$'")
                    
                    for elem in elementos[:10]:  # Limitar a 10 para performance
                        try:
                            texto = elem.text.strip()
                            if texto and len(texto) < 100:  # Evitar textos muy largos
                                print(f"         Texto: '{texto}'")
                                precio = extraer_precio_texto(texto)
                                if precio:
                                    print(f"         ✅ Precio encontrado: ${precio:,}")
                                    return precio, "ok"
                        except Exception:
                            continue
                except Exception as e:
                    print(f"      Error en dollar_search: {e}")
            
            elif estrategia.get('method') == 'pattern_search':
                # Buscar por patrones numéricos
                try:
                    # Buscar elementos con números que parezcan precios
                    elementos = driver.find_elements(By.XPATH, "//*[text()[contains(., '1') or contains(., '2') or contains(., '3') or contains(., '4') or contains(., '5')]]")
                    
                    for elem in elementos[:20]:
                        try:
                            texto = elem.text.strip()
                            if re.search(r'\d{3,6}', texto):  # Al menos 3 dígitos
                                precio = extraer_precio_texto(texto)
                                if precio:
                                    print(f"         ✅ Precio por patrón: ${precio:,} desde '{texto}'")
                                    return precio, "ok"
                        except Exception:
                            continue
                except Exception as e:
                    print(f"      Error en pattern_search: {e}")
            
            else:
                # Selectores CSS específicos
                for selector in estrategia.get('selectors', []):
                    try:
                        elementos = driver.find_elements(By.CSS_SELECTOR, selector)
                        print(f"      Selector '{selector}': {len(elementos)} elementos")
                        
                        for elem in elementos:
                            try:
                                texto = elem.text.strip()
                                if texto:
                                    precio = extraer_precio_texto(texto)
                                    if precio:
                                        print(f"         ✅ Precio con selector: ${precio:,}")
                                        return precio, "ok"
                            except Exception:
                                continue
                    except Exception as e:
                        print(f"      Error con selector {selector}: {e}")
        
        # Estrategia final: Inspeccionar el DOM completo
        print("   🔬 Estrategia final: análisis completo del DOM")
        try:
            # Obtener todo el texto de la página
            page_text = driver.find_element(By.TAG_NAME, "body").text
            
            # Buscar patrones de precio en todo el texto
            patrones_precio = re.findall(r'\$\s*(\d{1,3}(?:\.\d{3})*)', page_text)
            if patrones_precio:
                for precio_str in patrones_precio:
                    precio = int(precio_str.replace('.', ''))
                    if 100 <= precio <= 1000000:
                        print(f"         ✅ Precio en DOM completo: ${precio:,}")
                        return precio, "ok"
        except Exception as e:
            print(f"      Error en análisis DOM: {e}")
        
        print("   ❌ No se encontró precio con ninguna estrategia")
        return None, "precio_no_encontrado"
        
    except TimeoutException:
        print("   ❌ Timeout cargando la página")
        return None, "timeout"
    except Exception as e:
        print(f"   ❌ Error inesperado: {e}")
        return None, "error"

def debug_page_content(driver):
    """Función para debuggear el contenido de la página"""
    try:
        print("\n🔍 DEBUG: Analizando contenido de la página...")
        
        # 1. Verificar si hay contenido JavaScript sin cargar
        page_source = driver.page_source
        if "You need to enable JavaScript" in page_source:
            print("❌ La página requiere JavaScript y no se ha cargado correctamente")
            return
        
        # 2. Buscar elementos con precio potenciales
        elementos_con_numeros = driver.find_elements(By.XPATH, "//*[text()[contains(., '$') or contains(., '1') or contains(., '2') or contains(., '3') or contains(., '4') or contains(., '5') or contains(., '6') or contains(., '7') or contains(., '8') or contains(., '9')]]")
        
        print(f"Elementos con números encontrados: {len(elementos_con_numeros)}")
        
        textos_interesantes = []
        for elem in elementos_con_numeros[:20]:  # Límite para evitar spam
            try:
                texto = elem.text.strip()
                if texto and len(texto) < 200:  # Evitar textos muy largos
                    if re.search(r'\d', texto):  # Contiene al menos un dígito
                        textos_interesantes.append(texto)
            except Exception:
                continue
        
        print("Textos con números encontrados:")
        for i, texto in enumerate(textos_interesantes[:10], 1):
            print(f"   {i}. '{texto}'")
        
        # 3. Verificar clases CSS comunes
        clases_comunes = ['price', 'Price', 'cost', 'currency', 'money', 'amount', 'valor']
        for clase in clases_comunes:
            elementos = driver.find_elements(By.XPATH, f"//*[contains(@class, '{clase}')]")
            if elementos:
                print(f"Elementos con clase '{clase}': {len(elementos)}")
                for elem in elementos[:3]:
                    try:
                        texto = elem.text.strip()
                        if texto:
                            print(f"   - '{texto}'")
                    except Exception:
                        continue
        
    except Exception as e:
        print(f"Error en debug: {e}")

# Función de prueba
# Reemplazar tu función obtener_precio() actual con obtener_precio_mejorado()
# También puedes usar debug_page_content() cuando necesites investigar problemas

def integrar_con_script_actual():
    """
    Para integrar estas mejoras con tu script actual:
    
    1. Reemplaza la función obtener_precio() con obtener_precio_mejorado()
    2. Reemplaza la función extraer_precio() con extraer_precio_texto()
    3. Aumenta el tiempo de espera en build_browser()
    4. Usa debug_page_content() cuando no encuentres precios
    
    Cambios específicos en scraper.py:
    - Línea ~140: Cambiar time.sleep(3) por time.sleep(8)
    - Línea ~159: Reemplazar la lógica de búsqueda por font-bold
    - Añadir manejo de errores más robusto
    """
    pass

def test_scraper():
    """Función para probar el scraper con una URL"""
    driver = build_browser()
    
    try:
        # URLs de prueba
        test_urls = [
            "https://www.jumbo.cl/pechuga-deshuesada-de-pollo-800-g-cuisine-and-co-1801136/p",
            "https://www.jumbo.cl/trutro-corto-de-pollo-canto-del-gallo-granel/p"
        ]
        
        for i, test_url in enumerate(test_urls, 1):
            print(f"\n{'='*50}")
            print(f"PRUEBA {i}/{len(test_urls)}")
            print(f"{'='*50}")
            
            # Intentar obtener precio
            precio, status = obtener_precio_mejorado(test_url, driver)
            
            if precio:
                print(f"\n✅ ÉXITO: Precio obtenido: ${precio:,}")
            else:
                print(f"\n❌ FALLO: {status}")
                # Hacer debug si falla
                debug_page_content(driver)
            
            if i < len(test_urls):
                print("Esperando 3 segundos antes de la siguiente prueba...")
                time.sleep(3)
            
    finally:
        driver.quit()

if __name__ == "__main__":
    test_scraper()
