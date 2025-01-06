CREATE OR REPLACE PROCEDURE dm.fill_account_balance_f (i_ondate DATE)
LANGUAGE plpgsql
AS
$$
DECLARE
    accounts_cur CURSOR FOR (
        SELECT 
            account_rk
        FROM
            ds.md_account_d
        WHERE i_ondate BETWEEN data_actual_date
            AND data_actual_end_date
    );

    prev_balance_out NUMERIC(23, 8) := 0;
    prev_balance_out_rub NUMERIC(23, 8) := 0;
    curr_account INTEGER;
    p_char_type CHAR(1);
    p_balance_out NUMERIC(23, 8) := 0;
    p_balance_out_rub NUMERIC(23, 8) := 0;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM dm.dm_account_balance_f) THEN
        CALL dm.fill_primary_data_account_balance_f ();
    END IF;

    OPEN accounts_cur;

    LOOP
        FETCH accounts_cur INTO curr_account;
        EXIT WHEN NOT FOUND;
        
        -- Находим тип счета
        SELECT char_type
        INTO p_char_type
        FROM ds.md_account_d 
        WHERE curr_account = account_rk
            AND i_ondate BETWEEN data_actual_date AND data_actual_end_date;

        -- Находим остаток за прошлый день, используем COALESCE для замены NULL на 0
        SELECT COALESCE(balance_out, 0), COALESCE(balance_out_rub, 0)
        INTO prev_balance_out, prev_balance_out_rub
        FROM dm.dm_account_balance_f
        WHERE EXTRACT(DAY FROM i_ondate::TIMESTAMP - on_date::TIMESTAMP) = 1
        AND account_rk = curr_account;

        -- Находим balance_out
        IF p_char_type = 'А' THEN 
            SELECT 
                prev_balance_out + debet_amount - credit_amount,
                prev_balance_out_rub + debet_amount_rub - credit_amount_rub
            INTO p_balance_out, p_balance_out_rub
            FROM dm.dm_account_turnover_f
            WHERE account_rk = curr_account;
        ELSIF p_char_type = 'П' THEN 
            SELECT 
                prev_balance_out - debet_amount + credit_amount,
                prev_balance_out_rub - debet_amount_rub + credit_amount_rub
            INTO p_balance_out, p_balance_out_rub
            FROM dm.dm_account_turnover_f
            WHERE account_rk = curr_account; 
        END IF;

        -- Вставка результатов в таблицу
        INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
        VALUES (
            i_ondate, 
            curr_account, 
            COALESCE(p_balance_out, 0),
            COALESCE(p_balance_out_rub, 0)
        );

    END LOOP;

    CLOSE accounts_cur;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        INSERT INTO logs.logs_dm VALUES (
            'ERROR', 
            NOW(), 
            'dm.fill_account_balance_f: ' || SQLERRM
        );
        RAISE;
END;
$$;
