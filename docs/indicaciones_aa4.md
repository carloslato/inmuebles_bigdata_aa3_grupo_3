Evaluación AA
**Aplicando tecnologías para las soluciones Big Data II**
Indicaciones para el informe, prototipo funcional y exposición

```
UD. Diseño de Soluciones de Big Data
Documento de orientación para equipos de trabajo
2026
```

```
Evaluación AA4: Indicaciones Generales
```
**1. Propósito de la actividad**

```
En equipos, los estudiantes deberán evolucionar el proyecto trabajado en la AA3 hacia una
solución Big Data integral. La propuesta debe incorporar procesamiento batch,
procesamiento streaming, persistencia o integración con MongoDB, uso de GitHub y
visualización de resultados.
```
```
El objetivo es demostrar que el equipo puede integrar datos históricos, procesarlos con
Apache Spark, generar indicadores, trabajar eventos en tiempo real con Kafka y Spark
Structured Streaming, y presentar evidencias técnicas claras de la solución desarrollada.
```
**2. Modalidad de trabajo**

```
· La actividad se realizará en equipos de máximo cinco integrantes.
· La calificación será individual, considerando informe, prototipo funcional, exposición y
evidencias de participación.
· El docente podrá realizar preguntas aleatorias a cualquier integrante del equipo.
· Cada grupo debe mantener el caso trabajado en la AA3 o justificar claramente cualquier
ajuste realizado.
```
**IMPORTANTE: Todo integrante debe evidenciar participación tanto en la exposición
como en GitHub. Si un estudiante no expone, no participa, no asiste o no realiza la
actividad, podrá recibir calificación cero. Si no se verifica participación individual en
GitHub o en la exposición, la nota individual podrá ser mínima (01), según criterio docente.**

**3. Continuidad del caso trabajado**

```
Cada equipo debe partir del caso desarrollado anteriormente y ampliarlo hacia una solución
más completa. Para ello, debe explicar brevemente cuál fue el caso de la AA3, qué problema
busca resolver, qué datos utilizará o actualizará, cómo evoluciona hacia procesamiento con
Spark, MongoDB y streaming, y qué nuevo valor aporta la solución AA4.
```

**4. Datos de entrada**

```
Cada equipo deberá trabajar con mínimo cinco archivos de datos en total. Estos archivos
deben estar relacionados entre sí y permitir su integración durante el procesamiento. Dentro
de esos cinco archivos, uno debe funcionar como archivo principal del análisis, es decir,
aquel que contiene los registros centrales del caso. Los demás archivos deben aportar
información que permita enriquecer, clasificar, validar o contextualizar el análisis.
```
```
Requisito Mínimo requerido
Cantidad total de archivos históricos 5 archivos
Formatos diferentes 3 formatos
Archivo principal del análisis Mínimo 10,000 registros
Relación entre archivos Debe existir mediante IDs, fechas, categorías,
estados, zonas u otros campos
```
```
Los archivos pueden ser generados con Python, apoyo de inteligencia artificial, datos
públicos, simulación del grupo o una combinación de estos. No se aceptarán archivos
aislados que no tengan relación con el caso o que no aporten al procesamiento.
```
```
Formatos permitidos: CSV, JSON, TXT, LOG, XML, Parquet, Excel exportado a CSV u
otros formatos justificados por el equipo.
```
**5. Datos para streaming**

```
Además de los archivos históricos, cada equipo deberá simular un flujo de eventos en tiempo
real relacionado con su caso. Los eventos no tienen que ser iguales para todos los grupos;
cada equipo definirá el tipo de eventos y alertas según su problemática.
```
```
Elemento Cantidad mínima sugerida
Tipos de eventos 4 como mínimo
Eventos generados Entre 1,000 y 3,000 eventos
Reglas de alerta 2 como mínimo
```

```
Resúmenes streaming 2 como mínimo
```
```
Los eventos pueden representar cambios de estado, actividad de usuarios, registros
operativos, incidencias, transacciones, movimientos, solicitudes, alertas u otros elementos
propios del caso elegido.
```
**6. Tecnologías obligatorias**

```
· Docker como entorno de despliegue.
· Visual Studio Code como entorno de desarrollo.
· Apache Spark para procesamiento de datos.
· Spark DataFrames, Spark SQL y RDD.
· Kafka para la simulación de eventos en tiempo real.
· Spark Structured Streaming para el procesamiento streaming.
· MongoDB para la base de datos documental o persistencia de resultados.
· GitHub para versionamiento y evidencia del trabajo colaborativo.
· Python u otro lenguaje justificado por el equipo.
```
**7. Desarrollo que debe presentar cada equipo**

```
A. Definición del caso actualizado
```
```
· Nombre del caso.
· Problema identificado.
· Objetivo general.
· Actores involucrados.
· Justificación Big Data.
· Evolución desde AA3 hacia AA4.
· Parte batch y parte streaming del proyecto.
B. Descripción de los datos
```
```
Se debe incluir una tabla que indique archivo, formato, cantidad de registros, fuente y uso
dentro del proyecto.
```
**Archivo Formato Registros Fuente Uso dentro del proyecto**

archivo_1 CSV 10,000+ IA/Python/datos Archivo principal


```
públicos
```
archivo_2 JSON Según caso Simulado Enriquecimiento

archivo_3 TXT/LOG Según caso Simulado Eventos históricos o registros

archivo_4 Formato libre Según caso Simulado Datos maestros o contexto

archivo_5 Formato libre Según caso Simulado Complemento del análisis

Eventos
streaming

```
JSON/Kafka 1,000 a 3,000 Simulado Procesamiento en tiempo real
```
```
C. Arquitectura de la solución
```
```
Deben presentar un diagrama donde se observe el flujo general de datos: archivos
históricos, Spark, ETL, reportes, MongoDB, visualización, eventos simulados, Kafka y
Spark Structured Streaming.
```
```
D. Procesamiento batch con Spark
```
```
· Lectura de archivos.
· Limpieza de datos.
· Transformación de columnas.
· Integración de fuentes.
· Uso de DataFrames.
· Uso de Spark SQL.
· Uso de RDD.
· Generación de resultados o KPIs.
· Exportación de resultados.
E. Procesamiento streaming con Kafka
```
```
· Creación de un productor de eventos.
· Creación de un topic en Kafka.
· Envío de eventos simulados.
· Lectura con Spark Structured Streaming.
· Procesamiento por micro-batches.
· Generación de alertas o resúmenes.
· Salida en consola, archivo o MongoDB.
```

```
F. MongoDB
```
```
· Nombre de la base de datos.
· Colecciones creadas.
· Estructura de documentos.
· Carga de resultados procesados.
· Consultas básicas.
· Capturas de evidencia.
```
Los nombres de las colecciones deben adaptarse al caso de cada grupo. Como mínimo, se
recomienda considerar una colección principal, una colección de resultados o KPIs, una
colección de eventos streaming y una colección de alertas o registros críticos.

```
G. GitHub y trabajo colaborativo
```
```
Cada equipo deberá presentar evidencia del trabajo en GitHub. No basta con subir el
proyecto al final; debe evidenciarse el proceso colaborativo.
```
```
· Repositorio del proyecto.
· Integrantes o colaboradores.
· Ramas creadas.
· Commits realizados por integrante.
· Historial de cambios.
· Pull requests o merges, si corresponde.
· README del proyecto.
· Estructura de carpetas.
```
```
H. Visualización de resultados
```
```
Elemento Cantidad mínima
Gráficos o visualizaciones 3
Reportes o KPIs 5
Interpretaciones de resultados 3
```

```
Las visualizaciones pueden ser imágenes generadas con Python, capturas de notebooks,
dashboards simples o gráficos exportados.
```
```
I. Resultados e interpretación
```
```
· Qué resultado se obtuvo.
· Qué significa ese resultado.
· Cómo ayuda al problema planteado.
· Qué decisión podría tomar una organización con esa información.
· Qué limitaciones tiene la solución.
```
**8. Estructura del informe**

```
El informe deberá mantener el siguiente orden:
```
**1. Portada**

```
o Nombre de la actividad.
o Nombre del equipo.
o Integrantes.
o Caso elegido.
o Enlace público del repositorio github del proyecto creado.
```
**2. Introducción**

```
o Presentación breve del problema y propósito del trabajo.
```
**3. Caso actualizado y problemática**

```
o Descripción del caso.
o Problema central.
o Objetivo general.
o Justificación.
```

```
o Evolución desde AA3 hacia AA4.
```
**4. Datos utilizados**

```
o Cantidad de archivos.
o Formatos.
o Procedencia.
o Archivo principal.
o Relación entre los archivos.
o Datos simulados para streaming.
```
**5. Arquitectura Big Data propuesta**

```
o Diagrama de arquitectura.
o Componentes utilizados.
o Flujo batch.
o Flujo streaming.
```
**6. Procesamiento batch con Spark**

```
o Lectura de archivos.
o Limpieza.
o Transformaciones.
o Integración.
o Uso de DataFrames.
o Uso de Spark SQL.
o Uso de RDD.
o Resultados generados.
```
**7. Procesamiento streaming con Kafka**


```
o Eventos definidos.
o Topic utilizado.
o Productor de eventos.
o Spark Structured Streaming.
o Alertas o resúmenes.
o Evidencias de ejecución.
```
**8. Modelo de datos en MongoDB**

```
o Nombre de la base de datos.
o Colecciones.
o Atributos principales.
o Ejemplos de documentos.
o Consultas realizadas.
o Justificación del modelo documental.
```
**9. GitHub y trabajo colaborativo**

```
o Repositorio.
o Ramas.
o Commits.
o Pull requests o merges.
o Evidencia de participación por integrante.
```
**10. Visualizaciones y resultados**

· KPIs generados.

· Gráficos.

· Interpretación de resultados.


**11. Beneficios de la solución**

```
· Mínimo 5 beneficios, entre tangibles e intangibles.
```
12. **Métricas de viabilidad**

```
· Mínimo 3 métricas relacionadas con rendimiento, tiempo o esfuerzo.
```
13. **Conclusiones**

```
· Utilidad de la solución.
· Tecnologías integradas.
· Logros alcanzados.
· Posibles mejoras.
```
14. **Referencias**
15. **Anexos de evidencia**

```
· Capturas y pruebas de funcionamiento.
```
**9. Evidencias mínimas en anexos**

```
· Contenedores Docker activos.
· Archivos de entrada.
· Ejecución de Spark.
· Uso de RDD, DataFrames y Spark SQL.
· Reportes generados.
· Gráficos generados.
· Topic de Kafka.
· Productor enviando eventos.
· Spark Streaming procesando eventos.
· MongoDB con colecciones.
· Consultas en MongoDB.
```

```
· Repositorio GitHub.
· Ramas, commits y colaboradores.
```
**10.Exposición**

```
La exposición tendrá una duración de 15 a 20 minutos, incluyendo preguntas. Todos los
integrantes deben conocer el proyecto completo, ya que el docente podrá realizar preguntas a
cualquier miembro del equipo.
```
```
Orden Responsable Tema Tiemposugerido
```
```
1 Integrante 1 Caso, problema, objetivos y datos utilizados 3 min
2 Integrante 2 Arquitectura Big Data y flujo del dato 3 min
3 Integrante 3 Procesamiento batch con Spark: RDD, DataFrames y SQL 4 min
4 Integrante 4 MongoDB: modelo, colecciones, carga y consultas 3 min
5 Integrante 5 Kafka Streaming, visualizaciones, GitHub y conclusiones 4 min
```
**11.Criterios mínimos para una buena presentación**

```
· Caso coherente y bien explicado.
· Datos suficientes y relacionados entre sí.
· Procesamiento real con Spark.
· Uso claro de RDD, DataFrames y SQL.
· Streaming funcional con Kafka.
· MongoDB correctamente justificado.
· Visualizaciones interpretadas.
· GitHub con evidencia de trabajo colaborativo.
· Dominio del tema por todos los integrantes.
· Explicación clara del ciclo de vida del dato.
```
**12.Nombre de entrega**

```
El documento deberá subirse al campus digital con los nombres de los integrantes al inicio.
El archivo deberá llevar el siguiente formato:
```

```
GrupoX_Evidencia
```
**13.Documentos base**

```
Estas indicaciones se elaboran tomando como base la continuidad de la AA3 y los
lineamientos de evaluación de la AA4, considerando el uso de Spark, MongoDB, GitHub,
visualización, ETL, DataFrames, RDD, Spark SQL y streaming con Kafka.
```

