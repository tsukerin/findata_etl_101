CREATE OR REPLACE PROCEDURE dm.fill_account_turnover_f (i_ondate DATE) 
LANGUAGE plpgsql
AS 
$$
DECLARE 
    accounts_cur CURSOR FOR (
            SELECT 
                credit_account_rk AS accounts
            FROM ds.ft_posting_f
            WHERE oper_date = i_ondate
            UNION
            SELECT
                debet_account_rk AS accounts
            FROM ds.ft_posting_f
            WHERE oper_date = i_ondate
        );

    curr_account INTEGER;
    p_credit_amount NUMERIC(23, 8) := 0;
    p_credit_amount_rub NUMERIC(23, 8) := 0;
    p_debet_amount NUMERIC(23, 8) := 0;
    p_debet_amount_rub NUMERIC(23, 8) := 0;
    actual_course FLOAT := 1;
BEGIN
    OPEN accounts_cur;

    LOOP
        FETCH accounts_cur INTO curr_account;
        EXIT WHEN NOT FOUND;

        -- Находим актуальный курс
        SELECT COALESCE(MAX(reduced_cource), 1)
        INTO actual_course
        FROM ds.md_exchange_rate_d d
        WHERE d.currency_rk IN (
            SELECT currency_rk FROM ds.ft_balance_f WHERE account_rk = curr_account
        )
        AND i_ondate BETWEEN data_actual_date AND data_actual_end_date;
                
        -- Находим сумму проводок за дату расчета для CREDIT
        SELECT COALESCE(SUM(credit_amount), 0)
        INTO p_credit_amount
        FROM ds.ft_posting_f
        WHERE credit_account_rk = curr_account
          AND oper_date = i_ondate;

        -- Находим сумму проводок за дату расчета в рублях для CREDIT
        p_credit_amount_rub := p_credit_amount * actual_course;
            
        -- Находим сумму проводок за дату расчета для DEBET
        SELECT COALESCE(SUM(debet_amount), 0)
        INTO p_debet_amount
        FROM ds.ft_posting_f
        WHERE debet_account_rk = curr_account
          AND oper_date = i_ondate;

        -- Находим сумму проводок за дату расчета в рублях для DEBET
        p_debet_amount_rub := p_debet_amount * actual_course;

        -- Проверяем, нашлись ли проводки за указанную дату
        IF p_credit_amount > 0 OR p_debet_amount > 0 THEN
            -- Вставляем данные в итоговую таблицу
            INSERT INTO dm.dm_account_turnover_f VALUES (
                i_ondate, 
                curr_account, 
                p_credit_amount, 
                p_credit_amount_rub, 
                p_debet_amount, 
                p_debet_amount_rub
            );
        END IF;

    END LOOP;

    CLOSE accounts_cur;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        INSERT INTO dm.logs_dm VALUES (
            'ERROR', 
            NOW(), 
            'dm.fill_account_turnover_f: ' || SQLERRM
        );
        RAISE;
END;
$$;
