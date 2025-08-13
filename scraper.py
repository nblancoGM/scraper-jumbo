# -*- coding: utf-8 -*-
"""
Scraper específico para Jumbo.cl basado en la estructura real del sitio
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
    """Configuración específica para Jumbo.cl"""
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--window-size=1920,1080")
    
    # User agent específico para Chile
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    
    # Headers adicionales para parecer más legítimo
    options.add_argument("--accept-language=es-CL,es;q=0.9,en;q=0.8")
    
    # Perfil único
    profile_dir = os.path.join(tempfile.gettempdir(), f"chrome-profile-{uuid.uuid4()}")
    os.makedirs(profile_dir, exist_ok=True)
    options.add_argument(f"--user-data-dir={profile_dir}")
    
    chrome_bin = os.environ.get("CHROME_BIN", "").strip()
    if chrome_bin and os.path.exists(chrome_bin):
        options.binary_location = chrome_bin
    
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(120)
    
    # Scripts para ocultar automatización
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array")
    driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise")
    driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol")
    
    return driver

def extraer_precio_jumbo(texto):
    """Función específica para extraer precios del formato de Jumbo Chile"""
    if not texto or len(texto) > 200:
        return None
    
    # Limpiar el texto
    texto = texto.replace('\n', ' ').replace('\t', ' ').strip()
    texto = re.sub(r'\s+', ' ', texto)  # Múltiples espacios -> uno
    
    print(f"         DEBUG: Analizando texto: '{texto[:100]}'")
    
    # Patrones específicos para Jumbo Chile
    patrones = [
        # Formato principal: $1.234 o $ 1.234
        r'\$\s*([\d\.]+)(?!\s*(?:antes|original|normal|was))',
        
        # Formato con separadores de miles: 1.234
        r'(?<![\d\.])([\d]{1,3}(?:\.[\d]{3})+)(?!\s*(?:antes|original|kg|g|ml|lt|und|pack))',
        
        # Precio sin símbolo pero con contexto
        r'(?i)(?:precio|valor|cuesta|vale)\s*:?\s*([\d\.]+)',
        
        # Solo números de 3-6 dígitos (rango típico de precios)
        r'(?<![\d\.])([\d]{3,6})(?![\d\.]|(?:\s*(?:g|kg|ml|lt|cm|mm|und|pack|años|days|hrs)))',
        
        # Formatos específicos de e-commerce chileno
        r'CLP\s*([\d\.]+)',
        r'Precio\s*:?\s*\$?\s*([\d\.]+)',
        r'Total\s*:?\s*\$?\s*([\d\.]+)',
    ]
    
    for i, patron in enumerate(patrones, 1):
        matches = re.findall(patron, texto, re.IGNORECASE)
        if matches:
            print(f"         DEBUG: Patrón {i} encontró: {matches}")
            for match in matches:
                try:
                    # Limpiar el precio
                    precio_str = str(match).replace('.', '').replace(',', '')
                    precio = int(precio_str)
                    
                    # Validar rango de precios típicos en supermercado chileno
                    if 50 <= precio <= 500000:  # De $50 a $500.000
                        print(f"         DEBUG: Precio válido encontrado: ${precio:,}")
                        return precio
                    else:
                        print(f"         DEBUG: Precio fuera de rango: {precio}")
                except (ValueError, TypeError):
                    continue
    
    print("         DEBUG: No se encontró precio válido")
    return None

def obtener_precio_jumbo_especifico(url: str, driver: webdriver.Chrome, max_retries: int = 3) -> tuple:
    """
    Función específica para obtener precios de Jumbo.cl
    Maneja la estructura moderna de SPA con JavaScript
    """
    print(f"🌐 Procesando URL Jumbo: {url}")
    
    for intento in range(1, max_retries + 1):
        try:
            print(f"   Intento {intento}/{max_retries}")
            
            # Cargar la página
            driver.get(url)
            print("   📄 Página cargada, esperando JavaScript...")
            
            # Esperar que JavaScript se ejecute
            time.sleep(10)  # Tiempo suficiente para SPA
            
            # Verificar si la página se cargó correctamente
            page_source = driver.page_source
            if "You need to enable JavaScript" in page_source:
                print("   ❌ JavaScript no se ejecutó correctamente")
                if intento < max_retries:
                    time.sleep(5)
                    continue
                else:
                    return None, "javascript_error"
            
            # Esperar a que aparezca contenido de producto
            try:
                WebDriverWait(driver, 20).until(
                    lambda d: len(d.find_elements(By.TAG_NAME, "div")) > 10
                )
                print("   ✅ Contenido de la página cargado")
            except TimeoutException:
                print("   ⚠️ Timeout esperando contenido, pero continuando...")
            
            # ESTRATEGIA 1: Buscar selectores específicos de e-commerce
            selectores_precio = [
                # Selectores comunes de plataformas e-commerce
                '[data-testid*="price"]',
                '[data-cy*="price"]',
                '[class*="price"]',
                '[class*="Price"]',
                '[class*="currency"]',
                '[class*="money"]',
                '[class*="amount"]',
                '[class*="cost"]',
                '[id*="price"]',
                
                # Selectores específicos de VTEX (común en Chile)
                '.vtex-product-price',
                '.vtex-store-components',
                '.vtex-flex-layout',
                
                # Selectores generales con números
                'span[class*="bold"]',
                'div[class*="bold"]',
                'p[class*="bold"]',
                'strong',
                'b',
                
                # Selectores por estructura
                'main *',
                'article *',
                '[role="main"] *',
            ]
            
            print("   🔍 Buscando con selectores específicos...")
            for selector in selectores_precio[:10]:  # Limitar para performance
                try:
                    elementos = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elementos:
                        print(f"      Selector '{selector}': {len(elementos)} elementos")
                        
                        for elem in elementos[:15]:  # Limitar elementos por selector
                            try:
                                texto = elem.text.strip()
                                if texto and 2 <= len(texto) <= 50:  # Longitud razonable
                                    precio = extraer_precio_jumbo(texto)
                                    if precio:
                                        print(f"      ✅ Precio encontrado con selector: ${precio:,}")
                                        return precio, "ok"
                            except Exception:
                                continue
                except Exception as e:
                    continue
            
            # ESTRATEGIA 2: Búsqueda por contenido con $
            print("   🔍 Buscando elementos con símbolo $...")
            try:
                elementos_dollar = driver.find_elements(By.XPATH, "//*[contains(text(), '$')]")
                print(f"      Encontrados {len(elementos_dollar)} elementos con '$'")
                
                for elem in elementos_dollar[:20]:
                    try:
                        texto = elem.text.strip()
                        if texto and len(texto) <= 100:
                            precio = extraer_precio_jumbo(texto)
                            if precio:
                                print(f"      ✅ Precio encontrado con $: ${precio:,}")
                                return precio, "ok"
                    except Exception:
                        continue
            except Exception as e:
                print(f"      Error buscando $: {e}")
            
            # ESTRATEGIA 3: Análisis del DOM completo
            print("   🔍 Análisis completo del DOM...")
            try:
                # Obtener todos los textos de la página
                all_elements = driver.find_elements(By.XPATH, "//*[text()]")
                textos_con_numeros = []
                
                for elem in all_elements:
                    try:
                        texto = elem.text.strip()
                        if texto and re.search(r'\d', texto) and len(texto) <= 200:
                            textos_con_numeros.append(texto)
                    except Exception:
                        continue
                
                print(f"      Analizando {len(textos_con_numeros)} textos con números...")
                
                # Buscar precios en todos los textos
                for texto in textos_con_numeros[:50]:  # Limitar para performance
                    precio = extraer_precio_jumbo(texto)
                    if precio:
                        print(f"      ✅ Precio en análisis DOM: ${precio:,}")
                        return precio, "ok"
                        
            except Exception as e:
                print(f"      Error en análisis DOM: {e}")
            
            # ESTRATEGIA 4: JavaScript para obtener precios
            print("   🔍 Ejecutando JavaScript para buscar precios...")
            try:
                # Script para buscar precios dinámicamente
                js_script = """
                var textos = [];
                var elements = document.querySelectorAll('*');
                for (var i = 0; i < elements.length && textos.length < 100; i++) {
                    var el = elements[i];
                    if (el.innerText && el.innerText.trim() && 
                        (el.innerText.includes('$') || /\\d{3,6}/.test(el.innerText)) &&
                        el.innerText.length < 200) {
                        textos.push(el.innerText.trim());
                    }
                }
                return textos;
                """
                
                textos_js = driver.execute_script(js_script)
                print(f"      JavaScript encontró {len(textos_js)} textos potenciales")
                
                for texto in textos_js:
                    precio = extraer_precio_jumbo(texto)
                    if precio:
                        print(f"      ✅ Precio con JavaScript: ${precio:,}")
                        return precio, "ok"
                        
            except Exception as e:
                print(f"      Error ejecutando JavaScript: {e}")
            
            print(f"   ❌ No se encontró precio en intento {intento}")
            
            if intento < max_retries:
                wait_time = 3 + (intento * 2)
                print(f"   ⏳ Esperando {wait_time}s antes del siguiente intento...")
                time.sleep(wait_time)
                
        except Exception as e:
            print(f"   ❌ Error en intento {intento}: {type(e).__name__}: {e}")
            if intento < max_retries:
                time.sleep(5)
            
    print(f"   ❌ FALLO FINAL: No se encontró precio después de {max_retries} intentos")
    return None, "precio_no_encontrado"

def debug_jumbo_page(driver, url):
    """Función específica para debuggear páginas de Jumbo"""
    print(f"\n🔬 DEBUG COMPLETO para: {url}")
    
    try:
        driver.get(url)
        time.sleep(15)  # Espera larga para JavaScript
        
        # 1. Verificar carga de JavaScript
        page_source = driver.page_source
        print(f"   Tamaño del HTML: {len(page_source)} caracteres")
        
        if "You need to enable JavaScript" in page_source:
            print("   ❌ PROBLEMA: JavaScript no se ejecutó")
            return
        
        # 2. Verificar título y URL final
        try:
            titulo = driver.title
            url_final = driver.current_url
            print(f"   Título: {titulo}")
            print(f"   URL final: {url_final}")
        except Exception as e:
            print(f"   Error obteniendo título/URL: {e}")
        
        # 3. Contar elementos
        try:
            total_divs = len(driver.find_elements(By.TAG_NAME, "div"))
            total_spans = len(driver.find_elements(By.TAG_NAME, "span"))
            total_texto = len(driver.find_elements(By.XPATH, "//*[text()]"))
            print(f"   Elementos DIV: {total_divs}")
            print(f"   Elementos SPAN: {total_spans}")
            print(f"   Elementos con texto: {total_texto}")
        except Exception as e:
            print(f"   Error contando elementos: {e}")
        
        # 4. Buscar textos con números
        try:
            elementos_numeros = driver.find_elements(By.XPATH, "//*[text()[contains(., '1') or contains(., '2') or contains(., '3') or contains(., '4') or contains(., '5')]]")
            print(f"   Elementos con números: {len(elementos_numeros)}")
            
            # Mostrar algunos ejemplos
            ejemplos = []
            for elem in elementos_numeros[:20]:
                try:
                    texto = elem.text.strip()
                    if texto and len(texto) <= 100:
                        ejemplos.append(texto)
                except Exception:
                    continue
            
            print("   Ejemplos de textos con números:")
            for i, texto in enumerate(ejemplos[:10], 1):
                print(f"      {i}. '{texto}'")
                
        except Exception as e:
            print(f"   Error buscando números: {e}")
        
        # 5. Buscar elementos con $
        try:
            elementos_dollar = driver.find_elements(By.XPATH, "//*[contains(text(), '$')]")
            print(f"   Elementos con '$': {len(elementos_dollar)}")
            
            for i, elem in enumerate(elementos_dollar[:5], 1):
                try:
                    texto = elem.text.strip()
                    print(f"      ${i}: '{texto}'")
                except Exception:
                    continue
                    
        except Exception as e:
            print(f"   Error buscando $: {e}")
        
        # 6. Analizar clases CSS
        try:
            js_clases = """
            var clases = new Set();
            var elements = document.querySelectorAll('*');
            for (var i = 0; i < elements.length && clases.size < 50; i++) {
                var el = elements[i];
                if (el.className && typeof el.className === 'string') {
                    el.className.split(' ').forEach(function(clase) {
                        if (clase && (clase.toLowerCase().includes('price') || 
                                     clase.toLowerCase().includes('money') || 
                                     clase.toLowerCase().includes('cost') ||
                                     clase.toLowerCase().includes('currency'))) {
                            clases.add(clase);
                        }
                    });
                }
            }
            return Array.from(clases);
            """
            
            clases_precio = driver.execute_script(js_clases)
            if clases_precio:
                print(f"   Clases relacionadas con precio: {clases_precio}")
            else:
                print("   No se encontraron clases relacionadas con precio")
                
        except Exception as e:
            print(f"   Error analizando clases: {e}")
        
        # 7. Screenshot para debugging visual (opcional)
        try:
            screenshot_path = f"/tmp/jumbo_debug_{int(time.time())}.png"
            driver.save_screenshot(screenshot_path)
            print(f"   Screenshot guardado en: {screenshot_path}")
        except Exception as e:
            print(f"   No se pudo guardar screenshot: {e}")
            
    except Exception as e:
        print(f"   Error general en debug: {e}")

def test_jumbo_scraper():
    """Función de prueba específica para Jumbo"""
    driver = build_browser()
    
    try:
        # URLs de prueba reales de Jumbo
        test_urls = [
            "https://www.jumbo.cl/pechuga-deshuesada-de-pollo-800-g-cuisine-and-co-1801136/p",
            "https://www.jumbo.cl/trutro-corto-de-pollo-canto-del-gallo-granel/p",
            "https://www.jumbo.cl/filetillo-de-pollo-800-g-cuisine-and-co-1801137/p"
        ]
        
        for i, url in enumerate(test_urls, 1):
            print(f"\n{'='*80}")
            print(f"PRUEBA {i}/{len(test_urls)} - URL: {url}")
            print(f"{'='*80}")
            
            # Primer intento: obtener precio
            precio, status = obtener_precio_jumbo_especifico(url, driver)
            
            if precio:
                print(f"\n✅ ÉXITO: Precio obtenido: ${precio:,}")
            else:
                print(f"\n❌ FALLO: {status}")
                print("Ejecutando debug completo...")
                debug_jumbo_page(driver, url)
            
            if i < len(test_urls):
                print(f"\n⏳ Esperando 5 segundos antes de la siguiente prueba...")
                time.sleep(5)
                
    finally:
        print("\n🔒 Cerrando navegador...")
        driver.quit()

if __name__ == "__main__":
    test_jumbo_scraper()
