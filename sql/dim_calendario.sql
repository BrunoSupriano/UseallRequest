# ---------------- SQL ----------------
sql_create_dim_calendario = text("""
CREATE TABLE IF NOT EXISTS useall.dim_calendario (
    data DATE PRIMARY KEY,

    ano INT,
    mes INT,
    dia INT,

    ano_mes TEXT,
    ano_mes_atual TEXT,
    ano_mes_ordem INT,

    nome_mes TEXT,
    nome_mes_abrev TEXT,
    nome_dia TEXT,
    nome_dia_abrev TEXT,

    dia_semana INT,
    semana_iso INT,
    ano_iso INT,
    trimestre INT,
    
    dia_mes_abr TEXT,

    is_fim_de_semana BOOLEAN,
    is_feriado BOOLEAN,
    nome_feriado TEXT
);
""")

sql_create_indices = text("""
CREATE INDEX IF NOT EXISTS idx_dim_calendario_data
    ON useall.dim_calendario (data);

CREATE INDEX IF NOT EXISTS idx_dim_calendario_ano_mes_ordem
    ON useall.dim_calendario (ano_mes_ordem);
""")

sql_atualiza_calendario = text("""
INSERT INTO useall.dim_calendario (
    data,
    ano,
    mes,
    dia,
    ano_mes,
    ano_mes_atual,
    ano_mes_ordem,
    nome_mes,
    nome_mes_abrev,
    nome_dia,
    nome_dia_abrev,
    dia_semana,
    semana_iso,
    ano_iso,
    trimestre,
    dia_mes_abr,
    is_fim_de_semana,
    is_feriado,
    nome_feriado
)
SELECT DISTINCT
    d::date AS data,

    EXTRACT(YEAR FROM d)::int AS ano,
    EXTRACT(MONTH FROM d)::int AS mes,
    EXTRACT(DAY FROM d)::int AS dia,

    TO_CHAR(d, 'YYYY/MM') AS ano_mes,

    CASE
        WHEN EXTRACT(YEAR FROM d) = EXTRACT(YEAR FROM CURRENT_DATE)
         AND EXTRACT(MONTH FROM d) = EXTRACT(MONTH FROM CURRENT_DATE)
        THEN 'Mês Atual'
        ELSE TO_CHAR(d, 'YYYY/MM')
    END AS ano_mes_atual,

    (EXTRACT(YEAR FROM d) * 100 + EXTRACT(MONTH FROM d))::int AS ano_mes_ordem,

    CASE EXTRACT(MONTH FROM d)
        WHEN 1 THEN 'Janeiro'
        WHEN 2 THEN 'Fevereiro'
        WHEN 3 THEN 'Março'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Maio'
        WHEN 6 THEN 'Junho'
        WHEN 7 THEN 'Julho'
        WHEN 8 THEN 'Agosto'
        WHEN 9 THEN 'Setembro'
        WHEN 10 THEN 'Outubro'
        WHEN 11 THEN 'Novembro'
        WHEN 12 THEN 'Dezembro'
    END AS nome_mes,

    CASE EXTRACT(MONTH FROM d)
        WHEN 1 THEN 'Jan'
        WHEN 2 THEN 'Fev'
        WHEN 3 THEN 'Mar'
        WHEN 4 THEN 'Abr'
        WHEN 5 THEN 'Mai'
        WHEN 6 THEN 'Jun'
        WHEN 7 THEN 'Jul'
        WHEN 8 THEN 'Ago'
        WHEN 9 THEN 'Set'
        WHEN 10 THEN 'Out'
        WHEN 11 THEN 'Nov'
        WHEN 12 THEN 'Dez'
    END AS nome_mes_abrev,

    CASE EXTRACT(ISODOW FROM d)
        WHEN 1 THEN 'Segunda-feira'
        WHEN 2 THEN 'Terça-feira'
        WHEN 3 THEN 'Quarta-feira'
        WHEN 4 THEN 'Quinta-feira'
        WHEN 5 THEN 'Sexta-feira'
        WHEN 6 THEN 'Sábado'
        WHEN 7 THEN 'Domingo'
    END AS nome_dia,

    CASE EXTRACT(ISODOW FROM d)
        WHEN 1 THEN 'Seg'
        WHEN 2 THEN 'Ter'
        WHEN 3 THEN 'Qua'
        WHEN 4 THEN 'Qui'
        WHEN 5 THEN 'Sex'
        WHEN 6 THEN 'Sáb'
        WHEN 7 THEN 'Dom'
    END AS nome_dia_abrev,

    EXTRACT(ISODOW FROM d)::int AS dia_semana,
    EXTRACT(WEEK FROM d)::int AS semana_iso,
    EXTRACT(ISOYEAR FROM d)::int AS ano_iso,
    EXTRACT(QUARTER FROM d)::int AS trimestre,

    TO_CHAR(d, 'DD/MM') AS dia_mes_abr,

    EXTRACT(ISODOW FROM d) IN (6,7) AS is_fim_de_semana,
    FALSE AS is_feriado,
    NULL AS nome_feriado
FROM (
    SELECT DISTINCT data::date AS d
    FROM useall.gold_requisicoes
    WHERE data IS NOT NULL
) x
ON CONFLICT (data) DO NOTHING;
""")

# ---------------- EXECUÇÃO ----------------
with engine.begin() as conn:
    conn.execute(sql_create_dim_calendario)
    conn.execute(sql_create_indices)
    conn.execute(sql_atualiza_calendario)