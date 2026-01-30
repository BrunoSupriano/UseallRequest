SELECT
    desc_almox,
    COUNT(*) FILTER (WHERE py_status_estoque = 'Ruptura')              AS ruptura,
    COUNT(*) FILTER (WHERE py_status_estoque = 'Sem Ruptura')          AS sem_ruptura,
    COUNT(*) FILTER (WHERE py_status_estoque = 'Sem Mínimo Cadastrado') AS sem_minimo_cadastrado,
    COUNT(*) AS total_itens
FROM useall.vw_gold_estoque
GROUP BY desc_almox
ORDER BY desc_almox;