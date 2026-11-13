"""
Genera datasets FICTICIOS de ventas de centros comerciales de Lima.
Los datos son inventados con fines académicos (no son cifras reales de
Jockey Plaza, Mall Plaza, etc.).

Incluye "suciedad" controlada para justificar la capa silver:
  - ventas duplicadas
  - montos nulos / negativos
  - categorías y medios de pago con mayúsculas/espacios inconsistentes
  - fechas en dos formatos

Uso:  python scripts/generar_datos.py
Salida: datasets/
"""
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)
OUT = Path(__file__).resolve().parent.parent / "datasets"
OUT.mkdir(exist_ok=True)

# ---------------------------------------------------------------- malls
MALLS = [
    # id, nombre, operador, distrito, area_arrendable_m2, anio_apertura
    (1, "Jockey Plaza", "Jockey Plaza", "Santiago de Surco", 175000, 1997),
    (2, "Mall Plaza Bellavista", "Mall Plaza", "Bellavista", 85000, 2008),
    (3, "Mall Plaza Comas", "Mall Plaza", "Comas", 70000, 2017),
    (4, "Mall Plaza Arequipa", "Mall Plaza", "Arequipa", 60000, 2010),
    (5, "Larcomar", "Parque Arauco", "Miraflores", 32000, 1998),
    (6, "MegaPlaza Independencia", "MegaPlaza", "Independencia", 110000, 2002),
    (7, "Plaza San Miguel", "Grupo Wiese", "San Miguel", 90000, 1976),
]
# peso relativo de ventas por mall
PESO_MALL = {1: 1.8, 2: 1.0, 3: 0.8, 4: 0.7, 5: 0.9, 6: 1.3, 7: 1.1}

with open(OUT / "centros_comerciales.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["id_mall", "nombre_mall", "operador", "distrito", "area_m2", "anio_apertura"])
    w.writerows(MALLS)

# ---------------------------------------------------------------- tiendas
RUBROS = {
    "Moda": (["Saga Falabella", "Ripley", "Zara", "H&M", "Oechsle", "Forever 21"], 160),
    "Tecnologia": (["iShop", "Samsung Store", "Hiraoka", "Claro", "Movistar"], 520),
    "Alimentos": (["KFC", "Bembos", "Starbucks", "Pardos Chicken", "Tanta", "La Lucha"], 45),
    "Supermercado": (["Plaza Vea", "Tottus", "Wong"], 110),
    "Hogar": (["Promart", "Sodimac", "Casaideas"], 230),
    "Entretenimiento": (["Cineplanet", "Cinemark", "Coney Park"], 55),
    "Salud y Belleza": (["Inkafarma", "Mifarma", "Sephora", "Natura"], 75),
    "Deportes": (["Nike", "Adidas", "Marathon", "Platanitos"], 210),
}
tiendas = []
tid = 100
for id_mall, *_ in MALLS:
    for rubro, (marcas, _) in RUBROS.items():
        for marca in random.sample(marcas, k=min(len(marcas), random.randint(2, 4))):
            tid += 1
            tiendas.append({
                "id_tienda": tid,
                "nombre_tienda": marca,
                "rubro": rubro,
                "id_mall": id_mall,
                "piso": random.randint(1, 3),
                "area_m2": random.choice([40, 80, 120, 250, 600, 1500, 4000]),
                "fecha_apertura": str(date(2005, 1, 1) + timedelta(days=random.randint(0, 6500))),
            })
# JSON multilinea (como constructors.json del curso)
with open(OUT / "tiendas.json", "w", encoding="utf-8") as f:
    json.dump(tiendas, f, ensure_ascii=False, indent=1)

# ---------------------------------------------------------------- ventas
MEDIOS = ["Tarjeta Credito", "Tarjeta Debito", "Efectivo", "Yape", "Plin"]
INI, FIN = date(2025, 1, 1), date(2025, 12, 31)
dias = (FIN - INI).days + 1
ESTACIONAL = {1: 0.9, 2: 0.85, 3: 0.9, 4: 0.95, 5: 1.05, 6: 1.1, 7: 1.25,
              8: 1.0, 9: 0.95, 10: 1.0, 11: 1.1, 12: 1.6}

ventas = []
vid = 1_000_000
for t in tiendas:
    ticket_base = RUBROS[t["rubro"]][1]
    n = int(random.randint(700, 1500) * PESO_MALL[t["id_mall"]])
    for _ in range(n):
        d = INI + timedelta(days=random.randrange(dias))
        if random.random() > ESTACIONAL[d.month] / 1.6:  # más ventas en meses altos
            d = INI + timedelta(days=random.randrange(dias))
        vid += 1
        cant = random.choices([1, 2, 3, 4], weights=[60, 25, 10, 5])[0]
        monto = round(random.lognormvariate(0, 0.45) * ticket_base * cant, 2)
        medio = random.choice(MEDIOS)
        cat = t["rubro"]
        fecha = d.isoformat()
        # --- suciedad controlada
        r = random.random()
        if r < 0.01:
            monto = ""                     # nulo
        elif r < 0.015:
            monto = -monto                 # devolución mal cargada
        if random.random() < 0.05:
            medio = medio.upper() + "  "  # formato inconsistente
        if random.random() < 0.04:
            cat = " " + cat.lower()
        if random.random() < 0.03:
            fecha = d.strftime("%d/%m/%Y")  # otro formato de fecha
        ventas.append([vid, fecha, t["id_tienda"], t["id_mall"], cat, cant, monto, medio])

# duplicados (~1%)
ventas += random.sample(ventas, k=len(ventas) // 100)
random.shuffle(ventas)

# se parte en 2 archivos (ingesta incremental con Auto Loader)
cab = ["id_venta", "fecha_venta", "id_tienda", "id_mall", "categoria", "cantidad", "monto_soles", "medio_pago"]
mitad = len(ventas) // 2
for i, chunk in enumerate([ventas[:mitad], ventas[mitad:]], start=1):
    with open(OUT / f"ventas_2025_parte{i}.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cab)
        w.writerows(chunk)

# ---------------------------------------------------------------- afluencia diaria
with open(OUT / "afluencia_visitantes.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["fecha", "id_mall", "visitantes"])
    for i in range(dias):
        d = INI + timedelta(days=i)
        finde = 1.35 if d.weekday() >= 5 else 1.0
        for id_mall, *_ in MALLS:
            base = 1600 * PESO_MALL[id_mall]
            w.writerow([d.isoformat(), id_mall, int(base * finde * ESTACIONAL[d.month] * random.uniform(0.85, 1.15))])

print(f"malls={len(MALLS)} tiendas={len(tiendas)} ventas={len(ventas)} -> {OUT}")
