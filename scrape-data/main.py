from scrapling.spiders import Spider, Request, Response
import re
import json
import base64


class AdondeVivirSpider(Spider):
    """Spider para adondevivir.com - Departamentos en venta en Lima"""
    name = "adondevivir"
    start_urls = ["https://www.adondevivir.com/departamentos-en-venta-en-lima.html"]
    concurrent_requests = 5

    max_items = 100  # <-- LIMITE (None para ilimitado)
    scraped_count = 0

    async def parse(self, response: Response):
        """Procesa la página de listado de adondevivir"""
        cards = response.css('.postingsList-module__card-container')
        
        for card in cards:
            item = {}
            
            # Extraer precio
            price_el = card.css('.postingPrices-module__price')
            item["precio"] = price_el.css('::text').get("").strip() if price_el else ""
            
            # Extraer características (m², dormitorios, baños)
            features_el = card.css('h3[data-qa="POSTING_CARD_FEATURES"]')
            if features_el:
                spans = features_el.css('span::text').getall()
                item["caracteristicas"] = " | ".join(s.strip() for s in spans if s.strip())
            else:
                item["caracteristicas"] = ""
            
            # Extraer ubicación
            location_el = card.css('.postingLocations-module__location-text')
            item["ubicacion"] = location_el.css('::text').get("").strip() if location_el else ""
            
            # Extraer dirección
            address_el = card.css('.postingLocations-module__location-address')
            item["direccion"] = address_el.css('::text').get("").strip() if address_el else ""
            
            # Extraer descripción
            desc_el = card.css('.postingCard-module__posting-description')
            item["descripcion"] = desc_el.css('::text').get("").strip() if desc_el else ""
            
            # Extraer tipo de publicación (DEVELOPMENT / PROPERTY)
            layout = card.css('.postingCardLayout-module__posting-card-layout')
            item["tipo_publicacion"] = layout.css('::attr(data-posting-type)').get("") if layout else ""
            
            # Extraer URL del anuncio
            if layout:
                item["url"] = "https://www.adondevivir.com" + layout.css('::attr(data-to-posting)').get("")
            else:
                item["url"] = ""
            
            # Extraer datos adicionales del JSON-LD (datos estructurados)
            jsonld = card.css('script[type="application/ld+json"]')
            if jsonld:
                try:
                    data = json.loads(jsonld.css('::text').get("{}"))
                    obj = data.get("object", {})
                    item["nombre"] = obj.get("name", "")
                    item["descripcion_jsonld"] = obj.get("description", "")
                    item["dormitorios"] = obj.get("numberOfBedrooms", "")
                    item["banios"] = obj.get("numberOfBathroomsTotal", "")
                    
                    floor = obj.get("floorSize", {})
                    if floor:
                        item["area"] = floor.get("unitText", "")
                    
                    addr = obj.get("address", {})
                    if addr:
                        item["ciudad"] = addr.get("addressLocality", "")
                        if not item["direccion"]:
                            item["direccion"] = addr.get("streetAddress", "")
                    
                    geo = obj.get("geo", {})
                    if geo:
                        item["latitud"] = geo.get("latitude", "")
                        item["longitud"] = geo.get("longitude", "")
                except (json.JSONDecodeError, AttributeError):
                    pass
            
            # Extraer extras (etiquetas como "Áreas verdes", "Parrilla", etc.)
            extras = card.css('.postingCard-module__pill-item-feature::text').getall()
            if extras:
                item["extras"] = ", ".join(e.strip() for e in extras)
            
            self.scraped_count += 1
            yield item
        
        # paginación SOLO si aun no llegas al limite
        if self.max_items and self.scraped_count >= self.max_items:
            return
        
        # Paginación de adondevivir
        # Estructura: <div class="paging-module__container-paging">
        #   <a class="paging-module__page-arrow" data-qa="PAGING_NEXT" href="/departamentos-en-venta-en-lima-pagina-2.html">
        next_page = response.css('.paging-module__page-arrow[data-qa="PAGING_NEXT"]')
        if next_page:
            href = next_page.css('::attr(href)').get("")
            if href:
                yield Request(url=f"https://www.adondevivir.com{href}", callback=self.parse)


class LaEncontreSpider(Spider):
    """Spider para laencontre.com.pe - Departamentos en venta en Lima"""
    name = "laencontre"
    start_urls = ["https://www.laencontre.com.pe/venta/departamentos/lima"]
    concurrent_requests = 5

    max_items = 1000  # <-- LIMITE (None para ilimitado)
    scraped_count = 0

    async def parse(self, response: Response):
        """Procesa la página de listado de laencontre"""
        cards = response.css('li.serp-snippet.ad')
        
        for card in cards:
            item = {}
            
            # Extraer precio
            price_el = card.css('.price')
            item["precio"] = price_el.css('::text').get("").strip() if price_el else ""
            
            # Extraer título
            title_el = card.css('h2.title a')
            item["titulo"] = title_el.css('::text').get("").strip() if title_el else ""
            item["url"] = title_el.css('::attr(href)').get("") if title_el else ""
            if item["url"] and not item["url"].startswith("http"):
                item["url"] = f"https://www.laencontre.com.pe{item['url']}"
            
            # Extraer descripción
            desc_el = card.css('p.description')
            item["descripcion"] = desc_el.css('::text').get("").strip() if desc_el else ""
            
            # Extraer características (área, habitaciones, baños)
            area = card.css('.areaBuilt::text').get("")
            rooms = card.css('.rooms::text').get("")
            bathrooms = card.css('.bathrooms::text').get("")
            
            feats = []
            if area.strip(): feats.append(area.strip())
            if rooms.strip(): feats.append(f"{rooms.strip()} dorm.")
            if bathrooms.strip(): feats.append(f"{bathrooms.strip()} baños")
            item["caracteristicas"] = " | ".join(feats)
            
            # Extraer dirección (de metadatos schema.org)
            street = card.css('meta[itemprop="streetAddress"]::attr(content)').get("")
            locality = card.css('meta[itemprop="addressLocality"]::attr(content)').get("")
            region = card.css('meta[itemprop="addressRegion"]::attr(content)').get("")
            
            item["direccion"] = street if street else ""
            item["ubicacion"] = f"{locality}, {region}".strip(", ") if locality or region else ""
            
            # Extraer coordenadas
            item["latitud"] = card.css('meta[itemprop="latitude"]::attr(content)').get("")
            item["longitud"] = card.css('meta[itemprop="longitude"]::attr(content)').get("")
            
            # Extraer datos adicionales del id del anuncio y tipo
            item["id_anuncio"] = card.css('::attr(id)').get("")
            item["tipo"] = card.css('::attr(itemtype)').get("")
            
            self.scraped_count += 1
            yield item
        
        # paginación SOLO si aun no llegas al limite
        if self.max_items and self.scraped_count >= self.max_items:
            return
        # Paginación de laencontre
        # Estructura: <ul class="pagination">
        #   <li class="current"><span>1</span></li>
        #   <li><a class="linkFilter" title="2" href="/venta/departamentos/lima/p_2">2</a></li>
        #   ...
        #   <li class="next"><span class="spanFilter uExternalRefresh" id="BASE64_ID" title="Siguiente">Siguiente</span></li>
        #
        # Estrategia 1: Usar el span .spanFilter.uExternalRefresh cuyo id contiene el path base64 de la siguiente página
        next_span = response.css('li.next span.spanFilter.uExternalRefresh')
        if next_span:
            encoded_id = next_span.css('::attr(id)').get("")
            if encoded_id and len(encoded_id) > 10:
                try:
                    decoded = base64.b64decode(encoded_id).decode('utf-8')
                    next_url = f"https://www.laencontre.com.pe{decoded}"
                    yield Request(url=next_url, callback=self.parse)
                    return  # Salir si encontramos la siguiente página
                except Exception:
                    pass
        
        # Estrategia 2: Encontrar la página actual y tomar el siguiente enlace linkFilter
        current_li = response.css('li.current span::text').get("")
        if current_li:
            current_page = int(current_li.strip())
            next_page_link = response.css(f'li a.linkFilter[title="{current_page + 1}"]')
            if next_page_link:
                href = next_page_link.css('::attr(href)').get("")
                if href:
                    yield Request(url=f"https://www.laencontre.com.pe{href}", callback=self.parse)
                    return
        
        # Estrategia 3: Tomar el primer linkFilter cuyo title sea numérico (salta al más cercano)
        all_links = response.css('ul.pagination a.linkFilter')
        for link in all_links:
            href = link.css('::attr(href)').get("")
            title = link.css('::attr(title)').get("")
            if href and title and title.isdigit() and title != current_li.strip():
                yield Request(url=f"https://www.laencontre.com.pe{href}", callback=self.parse)
                break


class InfoCasasSpider(Spider):
    """Spider para infocasas.com.pe - Departamentos en venta en Lima"""
    name = "infocasas"
    start_urls = ["https://www.infocasas.com.pe/venta/departamentos/lima"]
    concurrent_requests = 5
    max_pages = 25  # Límite de seguridad para evitar bucles infinitos

    async def parse(self, response: Response):
        """Procesa la página de listado de infocasas"""
        cards = response.css('.listingCard')
        
        # Si no hay tarjetas en la página, asumimos que llegamos al final
        if not cards:
            return
        
        for card in cards:
            item = {}
            
            # Extraer precio
            price_el = card.css('.lc-price .main-price, .lc-price .heading')
            item["precio"] = price_el.css('::text').get("").strip() if price_el else ""
            
            # Extraer título
            title_el = card.css('.lc-title')
            item["titulo"] = title_el.css('::text').get("").strip() if title_el else ""
            
            # Extraer enlace
            link_el = card.css('a.lc-data')
            item["url"] = link_el.css('::attr(href)').get("") if link_el else ""
            if item["url"] and not item["url"].startswith("http"):
                item["url"] = f"https://www.infocasas.com.pe{item['url']}"
            
            # Extraer descripción
            desc_el = card.css('.lc-description')
            item["descripcion"] = desc_el.css('::text').get("").strip() if desc_el else ""
            
            # Extraer ubicación (el <strong> dentro de lc-data)
            location_el = card.css('strong.lc-location')
            item["ubicacion"] = location_el.css('::text').get("").strip() if location_el else ""
            
            # Extraer propietario/agencia
            owner_el = card.css('.lc-owner-name')
            item["agencia"] = owner_el.css('::text').get("").strip() if owner_el else ""
            
            # Extraer tipología (dormitorios, baños, área)
            typology_items = card.css('.lc-typologyTag__item')
            tipo_detalle = []
            for ti in typology_items:
                text = ti.css('::text').getall()
                txt = " ".join(t.strip() for t in text if t.strip())
                if txt:
                    tipo_detalle.append(txt)
            item["tipologia"] = ", ".join(tipo_detalle)
            
            # Extraer gastos comunes
            gastos = card.css('.commonExpenses')
            item["gastos_comunes"] = gastos.css('::text').get("").strip() if gastos else ""
            
            # Extraer etiquetas
            tags = card.css('.lc-tags')
            etiquetas = []
            for tag in tags:
                tag_text = tag.css('::text').getall()
                txt = " ".join(t.strip() for t in tag_text if t.strip())
                if txt:
                    etiquetas.append(txt)
            item["etiquetas"] = ", ".join(etiquetas)
            
            yield item
        
        # Paginación de infocasas — construcción manual de URLs
        # Página 1: https://www.infocasas.com.pe/venta/departamentos/lima
        # Página 2+: https://www.infocasas.com.pe/venta/departamentos/lima/pagina{N}
        base_url = "https://www.infocasas.com.pe/venta/departamentos/lima"
        
        # Determinar la página actual desde la URL
        current_page = 1
        url_str = str(response.url)
        if "/pagina" in url_str:
            # Extraer el número de página de la URL (ej: .../pagina2)
            import re as regex
            match = regex.search(r'/pagina(\d+)', url_str)
            if match:
                current_page = int(match.group(1))
        
        # También se puede detectar desde el paginador activo
        active_text = response.css('li.ant-pagination-item-active::text').get("")
        if active_text and active_text.strip().isdigit():
            current_page = int(active_text.strip())
        
        next_page = current_page + 1
        
        # Verificar que el paginador tiene un enlace a la siguiente página
        # para confirmar que no es la última página
        has_next = False
        
        # Buscar el enlace ">" (siguiente)
        next_links = response.css('.ant-pagination-item a')
        for link in next_links:
            link_text = link.css('::text').get("").strip()
            if link_text == ">":
                has_next = True
                break
        
        # También verificar si hay enlaces de página numerada más allá de la actual
        if not has_next:
            for link in next_links:
                link_text = link.css('::text').get("").strip()
                if link_text.isdigit() and int(link_text) > current_page:
                    has_next = True
                    break
        
        if has_next and next_page <= self.max_pages:
            next_url = f"{base_url}/pagina{next_page}"
            yield Request(url=next_url, callback=self.parse)


def run_all_spiders():
    """Ejecuta todos los spiders y guarda los resultados en archivos JSON"""
    print("=" * 60)
    print("INICIANDO SCRAPING DE PORTALES INMOBILIARIOS")
    print("=" * 60)
    
    resultados = {}
    
    # Spider 1: AdondeVivir
    print("\n[1/3] Scraping adondevivir.com...")
    spider1 = AdondeVivirSpider()
    result1 = spider1.start()
    print(f"  → Extraídos {len(result1.items)} inmuebles")
    resultados["adondevivir"] = result1
    
    # Spider 2: LaEncontre
    print("\n[2/3] Scraping laencontre.com.pe...")
    spider2 = LaEncontreSpider()
    result2 = spider2.start()
    print(f"  → Extraídos {len(result2.items)} inmuebles")
    resultados["laencontre"] = result2
    
    # Spider 3: InfoCasas
    print("\n[3/3] Scraping infocasas.com.pe...")
    spider3 = InfoCasasSpider()
    result3 = spider3.start()
    print(f"  → Extraídos {len(result3.items)} inmuebles")
    resultados["infocasas"] = result3
    
    # Guardar resultados individuales
    for nombre, resultado in resultados.items():
        filename = f"inmuebles_{nombre}.json"
        resultado.items.to_json(filename)
        print(f"  ✓ Guardado: {filename}")
    
    # Guardar un archivo consolidado
    todos = []
    for nombre, resultado in resultados.items():
        for item in resultado.items:
            item["portal"] = nombre
            todos.append(item)
    
    import json as json_lib
    with open("inmuebles_todos.json", "w", encoding="utf-8") as f:
        json_lib.dump(todos, f, ensure_ascii=False, indent=2)
    print(f"  ✓ Guardado consolidado: inmuebles_todos.json ({len(todos)} registros totales)")
    
    print("\n" + "=" * 60)
    print("SCRAPING COMPLETADO")
    print("=" * 60)
    
    return resultados


if __name__ == "__main__":
    run_all_spiders()