-- Заполнение витрины DM.DM_ACCOUNT_BALANCE_F первичными данными за самую раннюю дату - 2017-12-31

CREATE OR REPLACE PROCEDURE dm.fill_primary_data_account_balance_f ()
LANGUAGE plpgsql
AS
$$
DECLARE
    accounts_cur CURSOR FOR (
        SELECT 
            account_rk
        FROM
            ds.ft_balance_f
    );

    p_on_date DATE := '2017-12-31';
    curr_account INTEGER;
    actual_course FLOAT := 1;
    p_balance_out NUMERIC(23, 8) := 0;
    p_balance_out_rub NUMERIC(23, 8) := 0;
BEGIN
    INSERT INTO logs.logs_dm VALUES (
            'INFO', 
            NOW(), 
            'Выполнение dm.fill_primary_data_account_balance_f' 
        );

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
        );
                
        -- Находим balance_out
        SELECT balance_out
            INTO p_balance_out
        FROM ds.ft_balance_f
        WHERE curr_account = account_rk;

        -- Находим balance_out_rub
        p_balance_out_rub := p_balance_out * actual_course;

        INSERT INTO dm.dm_account_balance_f VALUES (
            p_on_date, 
            curr_account, 
            p_balance_out,
            p_balance_out_rub
        );

    END LOOP;

    CLOSE accounts_cur;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        INSERT INTO logs.logs_dm VALUES (
            'ERROR', 
            NOW(), 
            'dm.fill_primary_data_account_balance_f: ' || SQLERRM
        );
        RAISE;
END;
$$;