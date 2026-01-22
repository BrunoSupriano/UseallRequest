CREATE OR REPLACE VIEW useall.vw_gold_filiais_uf AS
SELECT 
    idfilial,
    datahoraalt,
    idempresa,
    matriz,
    apelido,
    CASE 
        -- Rio Grande do Sul
        WHEN idfilial IN (393, 336, 337, 558, 387) THEN 'RS'
        
        -- Bahia
        WHEN idfilial = 520 THEN 'BA'
        
        -- Distrito Federal
        WHEN idfilial = 404 THEN 'DF'
        
        -- Santa Catarina (Mapeamento explícito dos IDs atuais)
        WHEN idfilial IN (342, 343, 381, 389, 334, 335, 339, 333, 341, 578, 390, 379, 344, 345, 346, 338) THEN 'SC'
        
        -- Caso surja um ID novo que não foi tratado acima
        ELSE '*NOVA'
    END AS uf
FROM useall.gold_filiais;