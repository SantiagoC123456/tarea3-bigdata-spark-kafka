from pathlib import Path
import csv
import html

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, sum as spark_sum, when


BASE = Path(__file__).resolve().parent
DATA = BASE / "data" / "winequality-white.csv"
OUTPUT = BASE / "resultados"


def crear_grafica_svg(rows, ruta):
    ancho, alto = 900, 520
    margen_izq, margen_sup = 90, 90
    area_ancho, area_alto = 740, 330

    valores = [float(row["alcohol_promedio"]) for row in rows]
    maximo = max(valores) if valores else 1
    escala = maximo * 1.15

    partes = [
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{ancho}" height="{alto}" viewBox="0 0 {ancho} {alto}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<text x="450" y="42" text-anchor="middle" '
        'font-family="Arial" font-size="23" font-weight="bold">'
        'Alcohol promedio por calidad del vino blanco</text>',
        f'<line x1="{margen_izq}" y1="{margen_sup + area_alto}" '
        f'x2="{margen_izq + area_ancho}" y2="{margen_sup + area_alto}" '
        'stroke="black"/>',
        f'<line x1="{margen_izq}" y1="{margen_sup}" '
        f'x2="{margen_izq}" y2="{margen_sup + area_alto}" stroke="black"/>',
        f'<text x="28" y="260" transform="rotate(-90 28 260)" '
        'text-anchor="middle" font-family="Arial" font-size="16">'
        'Alcohol promedio (%)</text>',
        f'<text x="460" y="490" text-anchor="middle" '
        'font-family="Arial" font-size="16">Calidad (puntaje)</text>',
    ]

    cantidad = max(len(rows), 1)
    espacio = area_ancho / cantidad
    barra_ancho = min(42, espacio * 0.65)

    for indice, row in enumerate(rows):
        valor = float(row["alcohol_promedio"])
        calidad = int(row["quality"])
        x = margen_izq + indice * espacio + (espacio - barra_ancho) / 2
        altura_barra = (valor / escala) * area_alto
        y = margen_sup + area_alto - altura_barra

        partes.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{barra_ancho:.1f}" '
            f'height="{altura_barra:.1f}" fill="#3977a8"/>'
        )
        partes.append(
            f'<text x="{x + barra_ancho / 2:.1f}" '
            f'y="{y - 8:.1f}" text-anchor="middle" '
            f'font-family="Arial" font-size="12">{valor:.2f}</text>'
        )
        partes.append(
            f'<text x="{x + barra_ancho / 2:.1f}" '
            f'y="{margen_sup + area_alto + 22}" text-anchor="middle" '
            f'font-family="Arial" font-size="13">{calidad}</text>'
        )

    partes.append("</svg>")
    ruta.write_text("\n".join(partes), encoding="utf-8")


def main():
    if not DATA.exists():
        raise FileNotFoundError(f"No se encontró el dataset: {DATA}")

    OUTPUT.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("AnalisisBatchCalidadVino")
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        datos = (
            spark.read
            .option("header", True)
            .option("sep", ";")
            .option("inferSchema", True)
            .csv(str(DATA))
        )

        filas_originales = datos.count()
        print(f"FILAS_ORIGINALES={filas_originales}")
        print("ESQUEMA DEL DATASET:")
        datos.printSchema()

        nulos = datos.agg(
            *[
                spark_sum(when(col(nombre).isNull(), 1).otherwise(0)).alias(nombre)
                for nombre in datos.columns
            ]
        ).first().asDict()
        print("VALORES_NULOS:", {k: int(v or 0) for k, v in nulos.items()})

        sin_duplicados = datos.dropDuplicates()
        duplicados = filas_originales - sin_duplicados.count()

        limpios = (
            sin_duplicados
            .na.drop()
            .filter(
                col("quality").between(0, 10)
                & (col("alcohol") > 0)
                & (col("density") > 0)
            )
        )

        filas_limpias = limpios.count()
        print(f"FILAS_DUPLICADAS={duplicados}")
        print(f"FILAS_LIMPIAS={filas_limpias}")
        print(f"FILAS_DESCARTADAS={filas_originales - filas_limpias}")

        resumen = (
            limpios.groupBy("quality")
            .agg(
                count("*").alias("cantidad_vinos"),
                avg("alcohol").alias("alcohol_promedio"),
                avg("volatile acidity").alias("acidez_volatil_promedio"),
            )
            .orderBy("quality")
        )

        filas_resumen = resumen.collect()
        archivo_csv = OUTPUT / "resumen_por_calidad.csv"

        with archivo_csv.open("w", newline="", encoding="utf-8") as archivo:
            escritor = csv.writer(archivo)
            escritor.writerow(
                ["quality", "cantidad_vinos", "alcohol_promedio",
                 "acidez_volatil_promedio"]
            )
            for fila in filas_resumen:
                escritor.writerow(
                    [
          
              fila["quality"],
                        fila["cantidad_vinos"],
                        round(float(fila["alcohol_promedio"]), 3),
                        round(float(fila["acidez_volatil_promedio"]), 3),
                    ]
                )

        archivo_svg = OUTPUT / "alcohol_promedio_por_calidad.svg"
        crear_grafica_svg(filas_resumen, archivo_svg)

        print("RESUMEN POR CALIDAD:")
        resumen.show(truncate=False)
        print(f"CSV_GENERADO={archivo_csv}")
        print(f"GRAFICA_GENERADA={archivo_svg}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
