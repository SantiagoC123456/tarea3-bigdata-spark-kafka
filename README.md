\# Tarea 3 — Procesamiento de datos con Apache Spark



\## Análisis batch



Este proyecto analiza con PySpark el dataset de vinos blancos de UCI. Agrupa los registros por puntaje de calidad y calcula la cantidad de observaciones, el alcohol promedio y la acidez volátil promedio.



El análisis es descriptivo; no demuestra que una variable cause un puntaje determinado.



\## Fuente de datos



UCI Machine Learning Repository, \*Wine Quality\*:  

https://archive.ics.uci.edu/dataset/186/wine+quality



Cita: Cortez, P., Cerdeira, A., Almeida, F., Matos, T., \& Reis, J. (2009). \*Wine Quality\* \[Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C56S3T



Licencia indicada por UCI: Creative Commons Attribution 4.0 International (CC BY 4.0). Consulta: 29 de octubre de 2026.



\## Entorno de ejecución



\- Ubuntu 22.04

\- Java 11

\- PySpark 3.5.3

\- Spark local con un núcleo y 512 MB para el driver



\## Reproducir el análisis en la VM



En Ubuntu, ejecutar:



`cd /home/vboxuser/tarea3`



`export SPARK\_LOCAL\_IP=$(hostname -I | awk '{print $1}'); /home/vboxuser/.local/bin/spark-submit --master 'local\[1]' --driver-memory 512m batch\_analysis.py`



El programa lee `data/winequality-white.csv`, revisa el esquema y los valores nulos, elimina filas idénticas o incompletas y aplica los filtros definidos en el código.



\## Resultados generados



\- `resultados/resumen\_por\_calidad.csv`

\- `resultados/alcohol\_promedio\_por\_calidad.svg`



En la ejecución registrada se leyeron 4.898 filas y no se encontraron valores nulos. Se identificaron y eliminaron 937 filas idénticas; el resumen se calculó sobre 3.961 filas.



\*\*Nota metodológica:\*\* el dataset no incluye un identificador único de muestra. Por tanto, que dos filas sean idénticas no demuestra por sí solo que una sea un error. Esta decisión de limpieza debe tenerse en cuenta al interpretar los resultados.



El puntaje de calidad 9 tiene solo cinco registros. Sus promedios deben interpretarse con cautela.



\## Archivos principales



\- `batch\_analysis.py`: código del análisis batch.

\- `data/`: archivos del dataset descargado desde UCI.

\- `resultados/`: resumen CSV y gráfica SVG.

