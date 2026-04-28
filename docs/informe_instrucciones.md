**Aplicando tecnologías para las soluciones Big Data I**

1.  **Propósito de la actividad**
    

En equipos, deberán **diseñar e implementar un ecosistema Big Data funcional** a partir de una problemática propuesta por el grupo, aplicando **Apache Hadoop, Apache Spark y mongodb**, dentro de un entorno desplegado en **Docker**.

El propósito de esta actividad es que ustedes analicen una necesidad real o simulada, definan un caso de uso, gestionen datos heterogéneos, diseñen el procesamiento de datos y construyan una base de datos documental en mongodb, justificando técnica y funcionalmente su propuesta.

Esta actividad tendrá **continuidad en la siguiente evaluación**, por lo que el caso elegido deberá poder evolucionar posteriormente hacia un escenario de **streaming de datos**.

1.  **Modalidad de trabajo**
    

*   La actividad se realizará **en equipos**.
    
*   Cada equipo podrá tener **máximo 5 integrantes**.
    
*   La **calificación será individual**, en función del informe presentado, el prototipo funcional y la exposición grupal.
    
*   Durante la exposición, el docente podrá formular **preguntas aleatorias** a cualquier integrante del equipo.
    

1.  **Elección del caso**
    

Cada grupo deberá **proponer su propio caso de uso**, evitando repetir exactamente el mismo enfoque de otros equipos.

**El caso debe cumplir estas condiciones mínimas:**

*   Resolver una **problemática concreta**.
    
*   Ser coherente con un escenario de Big Data.
    
*   Permitir el uso de **5 archivos de entrada como mínimo**.
    
*   Incluir **al menos 3 formatos diferentes**.
    
*   Tener potencial de continuidad para una siguiente fase con **streaming**.
    
*   Permitir justificar el uso de **mongodb** como base de datos documental final.
    

**Ejemplos de dominios posibles**

Ventas, salud, transporte, educación, logística, reclamos, monitoreo, seguridad, servicios, atención al cliente, inventario, sensores, entre otros.

1.  **Datos que deberá utilizar cada equipo**
    

Cada grupo deberá trabajar con una combinación de datos provenientes de:

*   Datos generados con apoyo de IA,
    
*   Datos simulados por el grupo,
    
*   Y/o datos abiertos o públicos, cuando corresponda.
    

**Requisito mínimo obligatorio:**

*   **5 archivos como mínimo** 
    
*   **3 formatos diferentes como mínimo** 
    

**Ejemplo de combinación válida:**

*   2 archivos CSV
    
*   2 archivos JSON
    
*   1 archivo TXT o log
    

También podrían incluir XML, parquet, hojas tabulares exportadas, registros de eventos, entre otros, siempre que el grupo pueda justificar su uso.

**Los archivos deben permitir:**

*   Almacenamiento,
    
*   Lectura,
    
*   Limpieza,
    
*   Transformación,
    
*   Integración,
    
*   Y carga o persistencia de resultados.
    

1.  **Tecnologías obligatorias**
    

Todos los equipos deberán trabajar obligatoriamente con:

*   **Docker**, como entorno de despliegue del prototipo.
    
*   **Apache Hadoop**, para almacenamiento distribuido o simulación de ecosistema Big Data.
    
*   **Apache Spark**, para lectura, transformación y análisis de datos.
    
*   **Mongodb**, para el diseño e implementación de la base de datos documental final.
    

Además, podrán utilizar librerías complementarias en Python u otras herramientas justificadas, siempre que estén relacionadas con la propuesta.

1.  **Qué deberá hacer cada equipo**
    

Cada equipo deberá desarrollar lo siguiente:

**A. Definición del caso**

*   Nombre del caso
    
*   Problema identificado
    
*   Objetivo general de la solución
    
*   Actores o usuarios involucrados
    
*   Justificación del caso
    
*   Explicación breve de cómo este caso podría continuar luego con streaming 
    

**B. Análisis de requerimientos**

A partir de la problemática, el equipo deberá identificar:

*   Necesidades funcionales,
    
*   Necesidades técnicas,
    
*   Origen y naturaleza de los datos,
    
*   Posibles problemas de calidad de datos,
    
*   Requerimientos del procesamiento.
    

**C. Diseño de base de datos y modelo de datos**

Se deberá diseñar una base de datos en **mongodb**, explicando:

*   Nombre de la base de datos,
    
*   Colecciones definidas,
    
*   Atributos principales,
    
*   Relaciones lógicas entre colecciones,
    
*   Claves o identificadores,
    
*   Justificación del modelo documental,
    
*   Ejemplos de documentos.
    

Esta parte debe estar sustentada en el análisis previo y en Ingeniería de Requerimientos, porque es uno de los puntos centrales de la rúbrica.

**D. Diseño del procesamiento de datos**

Se deberá explicar:

*   Cómo ingresan los archivos al ecosistema,
    
*   Qué datos serán leídos con Spark,
    
*   Qué transformaciones se aplicarán,
    
*   Cómo se integrarán las fuentes,
    
*   Qué datos finales se almacenarán en mongodb.
    

Además, deberán presentar un **diagrama de arquitectura** donde se observe claramente el flujo del dato. Ese punto es clave para aspirar a niveles altos en el criterio de diseño del procesamiento.

**E. Prototipo funcional**

Cada equipo deberá mostrar un prototipo funcional en Docker que evidencie, como mínimo:

*   Entorno desplegado correctamente,
    
*   Acceso a las tecnologías requeridas,
    
*   Lectura de archivos,
    
*   Procesamiento básico con Spark,
    
*   Persistencia o carga de resultados en mongodb.
    

No basta con explicar la idea; debe existir una **demostración funcional mínima**.

**F. Beneficios del diseño**

Se deberán describir **5 beneficios tangibles e intangibles** del diseño propuesto, explicando por qué la solución resulta provechosa para el problema seleccionado. Este también es un criterio explícito de evaluación.

**G. Viabilidad de la propuesta**

Se deberán usar **3 métricas** relacionadas con:

*   Rendimiento,
    
*   Tiempo,
    
*   Esfuerzo.
    

Estas métricas deberán servir para sustentar la viabilidad de la arquitectura y del procesamiento de datos.

**H. Mejores prácticas**

El equipo deberá presentar **5 mejores prácticas de patrones de diseño para tecnologías Big Data**, basadas en una breve investigación nacional e internacional, redactadas en forma resumida y con ideas clave.

**I. Uso de Apache Spark**

Dentro de la solución deberán evidenciar el uso de:

*   **Apache Spark** 
    
*   **Spark RDD y/o dataframe** 
    
*   **Spark SQL** 
    

1.  **Estructura del informe**
    

El informe deberá mantener este orden:

**1\. Portada**

*   Nombre de la actividad
    
*   Nombre del equipo
    
*   Integrantes
    
*   Caso elegido
    

**2\. Introducción**

Breve presentación de la problemática y del propósito del trabajo.

**3\. Definición del caso y problema**

*   Descripción del caso,
    
*   Problema central,
    
*   Objetivo,
    
*   Justificación,
    
*   Continuidad futura con streaming.
    

**4\. Análisis de requerimientos**

*   Necesidades funcionales,
    
*   Necesidades técnicas,
    
*   Descripción del origen de datos,
    
*   Características del conjunto de archivos.
    

**5\. Descripción de los datos de entrada**

*   Cantidad de archivos,
    
*   Formatos utilizados,
    
*   Procedencia,
    
*   Uso previsto de cada archivo.
    

**6\. Diseño de la base de datos en mongodb**

*   Nombre de la BD,
    
*   Colecciones,
    
*   Atributos,
    
*   Identificadores,
    
*   Relaciones lógicas,
    
*   Ejemplo de documentos.
    

**7\. Diseño del procesamiento de datos**

*   Hadoop/HDFS
    
*   Spark
    
*   Flujo ETL o procesamiento
    
*   Diagrama de arquitectura
    

**8\. Frameworks y librerías utilizadas**

Justificación de por qué fueron elegidas según el caso y los datos.

**9\. Prototipo funcional en Docker**

*   Contenedores utilizados,
    
*   Componentes desplegados,
    
*   Evidencia de funcionamiento.
    

**10\. Beneficios del diseño**

Desarrollo de los 5 beneficios tangibles e intangibles.

**11\. Métricas y viabilidad**

Presentación y análisis de las 3 métricas.

**12\. Mejores prácticas de diseño Big Data**

Resumen de las 5 mejores prácticas investigadas.

**13\. Conclusiones**

Deben dejar claro que:

*   El diseño es útil para el problema,
    
*   La arquitectura es viable,
    
*   Y el caso puede continuar en la siguiente evaluación.
    

**14\. Referencias**

Fuentes utilizadas.

1.  **Exposición**
    

Cada grupo dispondrá de **15 a 20 minutos**, incluyendo preguntas. La exposición debe ser clara, técnica y ordenada. No se evaluará solo la lectura de diapositivas, sino la capacidad de explicar y sustentar la propuesta desarrollada.

**Se recomienda que la exposición siga este orden:**

1.  Caso y problemática
    
2.  Datos utilizados
    
3.  Modelo en mongodb 
    
4.  Arquitectura del ecosistema
    
5.  Procesamiento con Spark
    
6.  Prototipo funcional
    
7.  Beneficios
    
8.  Métricas y viabilidad
    
9.  Mejores prácticas
    
10.  Continuidad hacia streaming 
    
11.  **Entrega**
    

*   El documento deberá subirse al campus digital.
    
*   Debe incluir los nombres de los integrantes al inicio.
    
*   El archivo deberá llevar el nombre: **grupox\_Evidencia3**.
    

1.  **Criterios mínimos para aspirar a una buena calificación**
    

Para no quedarse en un nivel básico, el equipo debe procurar que su trabajo incluya:

*   Análisis real de necesidades técnicas y funcionales,
    
*   Modelo de datos bien estructurado,
    
*   Entidades, atributos, claves e interrelaciones lógicas,
    
*   Diagrama de arquitectura,
    
*   Selección justificada de frameworks y librerías,
    
*   5 beneficios bien sustentados,
    
*   3 métricas claras y coherentes,
    
*   5 mejores prácticas realmente relacionadas con Big Data,
    
*   Prototipo funcional demostrable,
    
*   Dominio del tema por parte de todos los integrantes.