DO $$
DECLARE
    r RECORD;
    gold_table TEXT;
BEGIN
    FOR r IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'useall'
          AND table_name LIKE 'silver_%'
    LOOP

        gold_table := replace(r.table_name, 'silver_', 'gold_');

        -- CASO ESPECIAL: SILVER_REQUISICOES
        IF r.table_name = 'silver_requisicoes' THEN

            -- cria se não existir
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS useall.%I AS
                 SELECT
                     *,
                     CASE status::int
                         WHEN 0  THEN ''Digitado''
                         WHEN 1  THEN ''Aberto''
                         WHEN 3  THEN ''Cancelado''
                         WHEN 10 THEN ''Parcial''
                         WHEN 11 THEN ''Atendido''
                         ELSE ''Desconhecido''
                     END AS py_desc_status
                 FROM useall.silver_requisicoes
                 WHERE false;',
                gold_table
            );

            -- limpa e reinsere
            EXECUTE format('TRUNCATE TABLE useall.%I;', gold_table);

            EXECUTE format(
                'INSERT INTO useall.%I
                 SELECT
                     *,
                     CASE status::int
                         WHEN 0  THEN ''Digitado''
                         WHEN 1  THEN ''Aberto''
                         WHEN 3  THEN ''Cancelado''
                         WHEN 10 THEN ''Parcial''
                         WHEN 11 THEN ''Atendido''
                         ELSE ''Desconhecido''
                     END AS py_desc_status
                 FROM useall.silver_requisicoes;',
                gold_table
            );

        -- DEMAIS TABELAS
        ELSE

            -- cria se não existir
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS useall.%I AS
                 SELECT * FROM useall.%I WHERE false;',
                gold_table,
                r.table_name
            );

            -- limpa e reinsere
            EXECUTE format('TRUNCATE TABLE useall.%I;', gold_table);

            EXECUTE format(
                'INSERT INTO useall.%I
                 SELECT * FROM useall.%I;',
                gold_table,
                r.table_name
            );

        END IF;

    END LOOP;
END $$;