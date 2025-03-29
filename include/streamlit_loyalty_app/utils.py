sql_1 = (
    """Agregacion sin temporalidad
        with
        puntos_ganados as (
            SELECT
                cliente_id,
                sum(puntos_ganados) as puntos_ganados
            FROM transacciones_table
        group by 1
        )
        , puntos_redimidos as (
            SELECT
                cliente_id,
                sum(puntos_redimidos) as puntos_redimidos
            FROM redenciones_table
            group by 1
        )
        SELECT
            c.cliente_id,
            c.nombre,
            g.puntos_ganados,
            r.puntos_redimidos,
            g.puntos_ganados - r.puntos_redimidos as saldo
        FROM clientes_table c
        LEFT JOIN puntos_ganados g on g.cliente_id = c.cliente_id
        LEFT JOIN puntos_redimidos r on r.cliente_id = c.cliente_id
    ;"""
)
sql_2 = (
    """Agregacion con temporalidad
        with
        puntos_ganados as (
            SELECT
                cliente_id,
                sum(puntos_ganados) as puntos_ganados
            FROM transacciones_table
            where fecha_transaccion::date >= CURRENT_DATE - INTERVAL '6 months'
        group by 1
        )
        , puntos_redimidos as (
            SELECT
                cliente_id,
                sum(puntos_redimidos) as puntos_redimidos
            FROM redenciones_table
            where fecha_redencion::date >= CURRENT_DATE - INTERVAL '6 months'
            group by 1
        )
        SELECT
            c.cliente_id,
            c.nombre,
            g.puntos_ganados,
            r.puntos_redimidos,
            g.puntos_ganados - r.puntos_redimidos as saldo
        FROM clientes_table c
        LEFT JOIN puntos_ganados g on g.cliente_id = c.cliente_id
        LEFT JOIN puntos_redimidos r on r.cliente_id = c.cliente_id
        order by saldo desc
    ;"""
)
sql_3 = (
    """Posibles transacciones fraudulentas
        with
        limites_clientes as (
            SELECT cliente_id, 3*avg(monto) limite
            FROM transacciones_table
            group by 1
        )
        SELECT t.*
        FROM transacciones_table t
        LEFT JOIN limites_clientes lc on t.cliente_id = lc.cliente_id
        WHERE t.monto > lc.limite
    ;"""
)
sql_4 = (
    """Transacciones con montos superiores al promedio por cliente
        with
        promedios as (
            SELECT cliente_id, avg(monto) promedio
            FROM transacciones_table
            group by 1
        )
        , promedio_cliente as (
            SELECT avg(promedio) promedio_por_cliente
            from promedios
        )
        SELECT t.*, p.*
        FROM transacciones_table t
        LEFT JOIN promedio_cliente p on 1=1
        WHERE t.monto > p.promedio_por_cliente
    ;"""
)
sql_5 = (
    """Evolución mensual de transacciones
        with
        transacciones as (
            SELECT
                date_trunc('month', fecha_transaccion) mes
                , count(*) cantidad_transacciones
                , sum(puntos_ganados) puntos_ganados
            FROM transacciones_table
            where fecha_transaccion::date >= CURRENT_DATE - INTERVAL '1 year'
            group by 1
        )
        , eventos_fraude as (
            SELECT
                date_trunc('month', fecha_evento) mes
                , count(*) cantidad_eventos_fraude
            from eventos_fraude_table
            where fecha_evento::date >= CURRENT_DATE - INTERVAL '1 year'
            group by 1
        )
        SELECT t.*, e.cantidad_eventos_fraude
        FROM transacciones t
        LEFT JOIN eventos_fraude e on t.mes = e.mes
    ;"""
)
