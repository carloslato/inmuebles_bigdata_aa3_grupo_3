# Análisis de Frecuencia de Palabras en Datos Inmobiliarios de Perú con Hadoop

Este proyecto demuestra un job automatizado de Hadoop MapReduce para el conteo de palabras, aplicado a un conjunto de datos de listados de propiedades inmobiliarias peruanas obtenidas mediante scraping. Está diseñado para analizar términos comunes, identificar distritos frecuentemente mencionados y extraer información relevante para una empresa inmobiliaria hipotética.

## Descripción del Proyecto

### Origen de los Datos
El proyecto utiliza un conjunto de datos compilado a partir de listados de propiedades en venta de portales inmobiliarios peruanos. Este conjunto de datos incluye:
*   Descripciones textuales de propiedades en venta (archivos `.txt`).
*   Folletos en formato PDF con detalles de proyectos (aunque el script actual de conteo de palabras procesa principalmente archivos `.txt` y `.csv`).
*   Datos adicionales extraidos de portales inmobiliarios en formato `.csv`.

### Caso de Uso Hipotético
El objetivo principal es analizar estos datos para comprender las tendencias del mercado y las preferencias de los consumidores. Específicamente, el análisis de frecuencia de palabras busca responder preguntas como:
*   "¿Cuáles son los **distritos** más mencionados en los listados de propiedades?"
*   "¿Cuáles son las **características** o **palabras clave** más comunes utilizadas en las descripciones?"
*   "¿Existen términos recurrentes relacionados con **precios** o **amenidades**?"
Esta información puede ayudar a una empresa inmobiliaria a tomar decisiones informadas sobre estrategias de marketing, desarrollo de propiedades e inversión.

## Configuración y Ejecución Local

Este proyecto está diseñado para ejecutarse localmente utilizando Docker y Docker Compose, automatizando la ejecución del job de Hadoop.

### Prerrequisitos
*   **Docker**: Tener Docker Desktop instalado y en ejecución.
*   **Docker Compose**: Generalmente incluido con Docker Desktop.

### Estructura de Archivos
El espacio de trabajo del proyecto contiene los siguientes archivos y directorios clave:
*   `docker-compose.yml`: Define los servicios de Docker para el clúster de Hadoop.
*   `namenode_entrypoint.sh`: Un script que automatiza todo el proceso del job de Hadoop cuando se inicia el contenedor `namenode`.
*   `src/WordCount.java`: El código Java que implementa la lógica de conteo de palabras de Hadoop MapReduce.
*   `input-data/`: Este directorio es donde debe colocar sus archivos de entrada `.txt` y `.csv` para el análisis.
*   `output-wordcounter/`: Este directorio se creará en su máquina host para almacenar los resultados del job de conteo de palabras.
*   `.gitignore`: Configurado para ignorar el directorio `output-wordcounter/`.

### Ejecución del Proyecto
1.  **Colocar Archivos de Entrada**: Agregue sus archivos `.txt` y `.csv` que contengan descripciones de propiedades u otros datos textuales relevantes en el directorio `input-data/`.
2.  **Iniciar el Entorno Hadoop y Ejecutar Job**: Abra una terminal en el directorio raíz del proyecto y ejecute el siguiente comando:
    ```bash
    docker compose up -d
    ```
    Este comando:
    *   Iniciará los servicios del clúster Hadoop definidos en `docker-compose.yml`.
    *   Ejecutará automáticamente el script `namenode_entrypoint.sh` dentro del contenedor `namenode`.
    *   El script copiará sus archivos de entrada, compilará el código Java, creará un JAR, ejecutará el job de Hadoop y copiará los resultados a `./output-wordcounter/` en su host.

### Acceso a los Resultados
El resultado del job de conteo de palabras se guardará en el directorio `./output-wordcounter/` en su máquina host. También puede ver los resultados directamente desde el contenedor `namenode` ejecutando:
```bash
docker exec namenode hdfs dfs -cat /output/part-r-00000
```

## Lógica de Conteo de Palabras con Hadoop

El proyecto utiliza una implementación estándar de Hadoop MapReduce (`src/WordCount.java`) para contar la frecuencia de palabras.
*   **Mapper**: Lee líneas de entrada (de archivos `.txt` y `.csv`), tokeniza las palabras y emite cada palabra con un conteo de 1.
*   **Reducer**: Agrega los conteos para cada palabra única y emite el mapeo final de palabra a frecuencia.
*   **Manejo de Entrada**: El job está configurado para leer desde el directorio HDFS `/input`, que se puebla con archivos de su directorio local `input-data/`.

## Para Informes y Presentaciones

### Contexto del Proyecto: Análisis de Datos Inmobiliarios en Perú

**La Idea y Objetivo Inicial**:
Este proyecto surge de la necesidad de adaptar un ejemplo de conteo de palabras con Hadoop para un trabajo de clase. Mi objetivo principal fue tomar datos reales de propiedades en venta en portales inmobiliarios de Perú, que recopilé mediante scraping y búsqueda manual. La idea era simular un análisis de datos para una empresa inmobiliaria hipotética, buscando entender patrones clave en el mercado.

**El Caso de Uso Hipotético**:
El análisis se centra en identificar información valiosa a partir de las descripciones de propiedades y otros datos textuales. Buscamos responder preguntas como:
*   ¿Cuáles son los **distritos** más mencionados en las propiedades en venta?
*   ¿Qué **características** o **palabras clave** se usan con mayor frecuencia en las descripciones?
*   ¿Existen términos recurrentes relacionados con **precios** o **amenidades**?
La meta es proporcionar a la empresa inmobiliaria información accionable para optimizar sus estrategias de marketing, desarrollo y inversión.

**Automatización del Proceso**:
Un aspecto clave de este proyecto fue la **automatización**. Inicialmente, la ejecución del job de Hadoop requería una serie de comandos manuales dentro de un contenedor Docker (como se detalla en `comandos_docker.md` y `comandos_ps.md`). El objetivo fue modificar el proyecto para que, con un solo comando (`docker compose up`), se ejecutara todo el proceso: desde el inicio del clúster Hadoop hasta la finalización del análisis de palabras. Esto se logró mediante la creación de un script de entrada (`namenode_entrypoint.sh`) y la configuración de `docker-compose.yml`.

### Detalles Técnicos
*   **Entorno**: Docker y Docker Compose para una configuración local sencilla y reproducible.
*   **Tecnología de Procesamiento**: Hadoop MapReduce para el análisis distribuido de grandes volúmenes de texto.
*   **Automatización**: El script `namenode_entrypoint.sh` orquesta todos los pasos: copia de datos, compilación de código Java, creación de JAR y ejecución del job de Hadoop.
*   **Archivos de Entrada**: El script procesa archivos `.txt` y `.csv` ubicados en el directorio `input-data/`.
*   **Salida**: Los resultados se guardan en `./output-wordcounter/` en su máquina local y se excluyen de Git.

### Sugerencias para la Presentación
*   **Demostración**: Muestre cómo ejecutar `docker compose up -d` y cómo el script de entrada automatiza todo el proceso.
*   **Datos y Caso de Uso**: Explique el origen de los datos (inmuebles en Perú) y el valor del análisis para una empresa inmobiliaria.
*   **Flujo de Hadoop**: Describa brevemente el modelo MapReduce (Mapper -> Reducer).
*   **Resultados**: Presente ejemplos de las palabras más frecuentes y los distritos identificados. Muestre la salida del archivo `part-r-00000` o el contenido de `./output-wordcounter/`.
*   **Código**: Resalte las partes clave de `WordCount.java` y `namenode_entrypoint.sh` que implementan la lógica y la automatización.

Este setup proporciona una forma robusta y automatizada de realizar análisis de frecuencia de palabras en sus datos inmobiliarios utilizando Hadoop.
