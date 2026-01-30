SELECT    desc_almox, count(*)
FROM useall.vw_status_estoque
GROUP BY desc_almox
ORDER BY desc_almox;