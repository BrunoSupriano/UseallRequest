CREATE OR REPLACE VIEW useall.vw_status_estoque AS
SELECT
    *,
    CASE
        -- sem mínimo cadastrado
        WHEN estoquemin IS NULL OR estoquemin = 0 THEN 'Sem Mínimo Cadastrado'

        -- ruptura (igual ao DAX)
        WHEN estoquemin > 0 AND saldodisponivel <= 0 THEN 'Ruptura'

        -- sem ruptura (igual ao DAX)
        WHEN estoquemin > 0 AND saldodisponivel > 0 THEN 'Sem Ruptura'

        -- fallback defensivo
        ELSE 'Sem Ruptura'
    END AS status_estoque
FROM useall.gold_estoque;