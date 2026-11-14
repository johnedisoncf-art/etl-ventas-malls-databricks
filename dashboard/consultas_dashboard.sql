-- 1) KPIs generales (widgets "Counter")
SELECT
  ROUND(SUM(total_ventas), 0)                       AS ventas_totales_soles,
  SUM(num_tickets)                                  AS tickets,
  ROUND(SUM(total_ventas) / SUM(num_tickets), 2)    AS ticket_promedio
FROM catalog_prod.golden.ventas_mensuales_mall;

-- 2) Ventas mensuales por centro comercial (gráfico de líneas: x=periodo, y=total_ventas, color=nombre_mall)
SELECT make_date(anio, mes, 1) AS periodo, nombre_mall, operador,
       total_ventas, num_tickets, ticket_promedio, crecimiento_vs_mes_anterior_pct
FROM catalog_prod.golden.ventas_mensuales_mall
ORDER BY periodo, nombre_mall;

-- 3) Top 5 tiendas por mall (tabla o barras)
SELECT nombre_mall, ranking_en_mall, nombre_tienda, rubro, total_ventas, participacion_mall_pct
FROM catalog_prod.golden.ranking_tiendas
WHERE ranking_en_mall <= 5
ORDER BY nombre_mall, ranking_en_mall;

-- 4) Conversión visitantes -> compras (barras: x=nombre_mall, y=conversion; filtro por fin de semana)
SELECT nombre_mall, es_fin_de_semana,
       SUM(visitantes)                                        AS visitantes,
       SUM(num_tickets)                                       AS tickets,
       ROUND(SUM(num_tickets) / SUM(visitantes) * 100, 2)     AS tasa_conversion_pct,
       ROUND(SUM(total_ventas) / SUM(visitantes), 2)          AS venta_por_visitante
FROM catalog_prod.golden.conversion_diaria_mall
GROUP BY nombre_mall, es_fin_de_semana;

-- 5) Medio de pago por rubro (barras apiladas)
SELECT rubro, medio_pago, monto
FROM catalog_prod.golden.ventas_rubro_medio_pago
UNPIVOT (monto FOR medio_pago IN (efectivo, plin, tarjeta_credito, tarjeta_debito, yape))
ORDER BY rubro;
