# Documentación del Scraper de Portales Inmobiliarios

## 🎯 Objetivo

Adaptar el scraper de muestra (`main.py`) para extraer datos de **3 portales inmobiliarios peruanos** que publican departamentos en venta en Lima. Los datos generados están pensados para ser procesados posteriormente con herramientas de **Big Data** (Spark, Hadoop, etc.).

---

## 🕸️ Portales Scrapeados

| # | Portal | URL | 
|---|--------|-----|
| 1 | **AdondeVivir** | https://www.adondevivir.com/departamentos-en-venta-en-lima.html |
| 2 | **LaEncontre** | https://www.laencontre.com.pe/venta/departamentos/lima |
| 3 | **InfoCasas** | https://www.infocasas.com.pe/venta/departamentos/lima |

---

## ⚙️ ¿Cómo se hizo?

### Stack tecnológico
- **Python 3** con la librería **Scrapling** (versión 0.4.7).
- Selectores **CSS** para extraer datos del HTML.
- Datos estructurados **JSON-LD** incrustados en las páginas (schema.org) para datos adicionales como coordenadas geográficas, número de dormitorios, baños, área, etc.

### Enfoque por portal

#### 1. AdondeVivir (`AdondeVivirSpider`)
- Las tarjetas de propiedades están dentro de `.postingsList-module__card-container`.
- Se extrae: precio, características (m², dormitorios), ubicación, dirección, descripción, tipo de publicación (PROPERTY / DEVELOPMENT), extras (etiquetas como "Áreas verdes", "Parrilla"), URL del anuncio.
- **Valor añadido**: Se parsea el bloque `<script type="application/ld+json">` dentro de cada tarjeta para extraer datos estructurados como coordenadas (latitud/longitud), número de dormitorios y baños, área en m², ciudad.
- **Paginación**: Sigue el enlace con `data-qa="PAGING_NEXT"` que apunta a la página siguiente.

#### 2. LaEncontre (`LaEncontreSpider`)
- Las tarjetas son elementos `<li class="serp-snippet ad">`.
- Se extrae: precio, título, descripción, área construida, número de habitaciones y baños.
- **Valor añadido**: Se utilizan los metadatos de schema.org (`<meta itemprop="...">`) para obtener dirección completa, ubicación (distrito, departamento) y coordenadas geográficas.
- **Paginación**: Detectada pero comentada — la estructura puede variar (a implementar según necesidad).

#### 3. InfoCasas (`InfoCasasSpider`)
- Las tarjetas son `<div class="listingCard">`.
- Se extrae: precio, título, descripción, ubicación, agencia/publicador, tipología (dormitorios, baños, m²), gastos comunes, etiquetas (Destacado, Proyecto, etc.).
- **Valor añadido**: Se parsean los elementos `.lc-typologyTag__item` que contienen los iconos y textos de tipo de unidad, dormitorios, baños y superficie.
- **Paginación**: Sigue los enlaces de paginación encontrados en `.ant-pagination-item`.

### Ejecución

Para ejecutar el scraper:

```bash
cd scrape-data
python main.py
```

Esto ejecutará los 3 spiders secuencialmente y generará los archivos de salida.

---

## 📂 Archivos de salida

| Archivo | Descripción |
|---------|-------------|
| `inmuebles_adondevivir.json` | Resultados individuales de AdondeVivir |
| `inmuebles_laencontre.json` | Resultados individuales de LaEncontre |
| `inmuebles_infocasas.json` | Resultados individuales de InfoCasas |
| `inmuebles_todos.json` | **Archivo consolidado** con todos los resultados, cada registro incluye el campo `"portal"` para identificar su origen |

---

## 🏗️ Estructura del JSON generado

A continuación se muestra la estructura de cada objeto JSON, con los campos comunes y específicos por portal.

### Campos comunes (presentes en todos los portales)

```json
{
  "precio": "S/ 392,773",
  "ubicacion": "Orrantia, San Isidro",
  "descripcion": "Una joya arquitectónica de 33 pisos...",
  "url": "https://www.adondevivir.com/propiedades/proyecto/..."
}
```

### Campos específicos por portal

#### AdondeVivir
```json
{
  "portal": "adondevivir",
  "precio": "S/ 392,773",
  "caracteristicas": "69 un. | 1 a 2 dorm. | 40 a 79 m² tot.",
  "ubicacion": "Orrantia, San Isidro",
  "direccion": "Calle Las Palmeras 291. San Isidro",
  "descripcion": "Una joya arquitectónica de 33 pisos...",
  "tipo_publicacion": "DEVELOPMENT",
  "url": "https://www.adondevivir.com/propiedades/proyecto/ememvein-palm-living-67055120.html",
  "nombre": "Proyecto vertical",
  "descripcion_jsonld": "Cristóbal es un proyecto en el corazón de Santiago de Surco...",
  "dormitorios": 4,
  "banios": 4,
  "area": "303m²",
  "ciudad": "Miraflores",
  "latitud": "-12.112695799999999",
  "longitud": "-77.04243769999999",
  "extras": "Áreas verdes, Parrilla"
}
```

#### LaEncontre
```json
{
  "portal": "laencontre",
  "precio": "$ 240,000",
  "titulo": "Departamento en Av. Ricardo Palma 1220, Miraflores, Perú",
  "descripcion": "SE VENDE DEPARTAMENTO EN EL DISTRITO DE MIRAFLORES...",
  "caracteristicas": "153.2m2 | 4 dorm. | 3 baños",
  "direccion": "Av. Ricardo Palma 1220, Miraflores, Perú",
  "ubicacion": "Miraflores, Lima, Lima Departamento",
  "latitud": "-12.1234334",
  "longitud": "-77.0176943",
  "id_anuncio": "01973d02-4852-71d3-8206-728de110da8f",
  "tipo": "https://schema.org/Residence",
  "url": "https://www.laencontre.com.pe/inmueble/48d8-828c-1973d88-728de110db15-7259"
}
```

#### InfoCasas
```json
{
  "portal": "infocasas",
  "precio": "Desde S/ 293.000",
  "titulo": "Casanova 151",
  "descripcion": "Ubicado a tan solo una cuadra del cruce de la Av. Arequipa...",
  "ubicacion": "Apartamento en Lince, Lima",
  "agencia": "GRUPO TALE",
  "tipologia": "1 Dorm, 1 Baño, 40 m²",
  "gastos_comunes": "+ S/ 350 mant.",
  "etiquetas": "Destacado, Proyecto",
  "url": "https://www.infocasas.com.pe/proyectos/casanova-151/14411"
}
```

### Archivo consolidado (`inmuebles_todos.json`)

Es un **array JSON** donde cada objeto tiene la estructura del portal correspondiente **más** el campo adicional:

```json
{
  "portal": "adondevivir",
  ... resto de campos del portal
}
```

---

## 📊 Uso con Big Data

Los archivos JSON generados pueden ser procesados con:

- **Apache Spark**: `spark.read.json("inmuebles_todos.json")`
- **Apache Hadoop**: Almacenar en HDFS y procesar con MapReduce o Hive.
- **Pandas**: Para análisis exploratorio rápido.
- **MongoDB**: Importar directamente como colección.

Ejemplo con PySpark:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Inmuebles").getOrCreate()
df = spark.read.json("inmuebles_todos.json")
df.printSchema()
df.groupBy("portal").count().show()
```

---

## 🔄 Notas adicionales

- Los spiders están configurados con `concurrent_requests = 5` para no sobrecargar los servidores.
- AdondeVivir tiene datos estructurados JSON-LD muy completos (lat/lng, dormitorios, baños, área), lo que lo convierte en la fuente más rica.
- LaEncontre expone datos vía metadatos schema.org en `<meta>` tags, lo que facilita la extracción.
- InfoCasas tiene una estructura moderna con componentes React y sus datos están bien organizados en clases CSS semánticas.
- La paginación está implementada para AdondeVivir e InfoCasas; para LaEncontre habría que ajustarla según la estructura real del sitio.