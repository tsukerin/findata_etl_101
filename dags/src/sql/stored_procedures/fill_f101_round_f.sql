CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f (i_ondate DATE)
LANGUAGE plpgsql
AS 
$$
DECLARE
    accounts_cur CURSOR FOR (
        SELECT
            ledger_account::VARCHAR
        FROM ds.md_ledger_account_s
    );

    p_from_date DATE := DATE_TRUNC('month', i_ondate) - INTERVAL '1 month';
    p_to_date DATE := DATE_TRUNC('month', i_ondate) - INTERVAL '1 day';
    p_chapter CHAR(1);
    p_ledger_account CHAR(5);
    p_characteristic CHAR(1);
    p_balance_in_rub NUMERIC(23, 8);
    p_balance_in_val NUMERIC(23, 8);
    p_balance_in_total NUMERIC(23, 8);
    p_turn_deb_rub NUMERIC(23, 8);
    p_turn_deb_val NUMERIC(23, 8);
    p_turn_deb_total NUMERIC(23, 8);
    p_turn_cre_rub NUMERIC(23, 8);
    p_turn_cre_val NUMERIC(23, 8);
    p_turn_cre_total NUMERIC(23, 8);
    p_balance_out_rub NUMERIC(23, 8);
    p_balance_out_val NUMERIC(23, 8);
    p_balance_out_total NUMERIC(23, 8);
BEGIN
    OPEN accounts_cur;

    INSERT INTO logs.logs_dm VALUES (
        'INFO', 
        NOW(), 
        'Выполнение dm.fill_f101_round_f' 
    );

    LOOP
        FETCH accounts_cur INTO p_ledger_account;
        EXIT WHEN NOT FOUND;

        SELECT chapter
        INTO p_chapter
        FROM ds.md_ledger_account_s
        WHERE ledger_account::VARCHAR = p_ledger_account;

        SELECT
            CASE 
                WHEN COUNT(DISTINCT char_type) = 1 THEN MAX(char_type) 
                ELSE 'N' 
            END
        INTO p_characteristic
        FROM ds.md_account_d
        WHERE SUBSTRING(account_number, 1, 5) = p_ledger_account;

        SELECT COALESCE(SUM(balance_out_rub), 0)
        INTO p_balance_in_rub
        FROM dm.dm_account_balance_f AS da
        JOIN ds.md_account_d AS d ON da.account_rk = d.account_rk
        WHERE EXTRACT(DAY FROM p_from_date::TIMESTAMP - on_date::TIMESTAMP) = 1
            AND currency_code IN ('810', '643')
            AND SUBSTRING(d.account_number, 1, 5) = p_ledger_account;

        SELECT COALESCE(SUM(balance_out_rub), 0)
        INTO p_balance_in_val
        FROM dm.dm_account_balance_f AS da
        JOIN ds.md_account_d AS d ON da.account_rk = d.account_rk
        WHERE EXTRACT(DAY FROM p_from_date::TIMESTAMP - on_date::TIMESTAMP) = 1
            AND currency_code NOT IN ('810', '643')
            AND SUBSTRING(d.account_number, 1, 5) = p_ledger_account;

        SELECT COALESCE(SUM(balance_out_rub), 0)
        INTO p_balance_in_total
        FROM dm.dm_account_balance_f AS da
        JOIN ds.md_account_d AS d ON da.account_rk = d.account_rk
        WHERE EXTRACT(DAY FROM p_from_date::TIMESTAMP - on_date::TIMESTAMP) = 1
            AND SUBSTRING(d.account_number, 1, 5) = p_ledger_account;

        SELECT COALESCE(SUM(debet_amount_rub), 0)
        INTO p_turn_deb_rub
        FROM dm.dm_account_turnover_f AS da
        JOIN ds.md_account_d AS d ON da.account_rk = d.account_rk
        WHERE on_date BETWEEN p_from_date AND p_to_date
            AND currency_code IN ('810', '643')
            AND SUBSTRING(d.account_number, 1, 5) = p_ledger_account;

        SELECT COALESCE(SUM(debet_amount_rub), 0)
        INTO p_turn_deb_val
        FROM dm.dm_account_turnover_f AS da
        JOIN ds.md_account_d AS d ON da.account_rk = d.account_rk
        WHERE on_date BETWEEN p_from_date AND p_to_date
            AND currency_code NOT IN ('810', '643')
            AND SUBSTRING(d.account_number, 1, 5) = p_ledger_account;

        SELECT COALESCE(SUM(debet_amount_rub), 0)
        INTO p_turn_deb_total
        FROM dm.dm_account_turnover_f AS da
        JOIN ds.md_account_d AS d ON da.account_rk = d.account_rk
        WHERE on_date BETWEEN p_from_date AND p_to_date
            AND SUBSTRING(d.account_number, 1, 5) = p_ledger_account;

        SELECT COALESCE(SUM(credit_amount_rub), 0)
        INTO p_turn_cre_rub
        FROM dm.dm_account_turnover_f AS da
        JOIN ds.md_account_d AS d ON da.account_rk = d.account_rk
        WHERE on_date BETWEEN p_from_date AND p_to_date
            AND currency_code IN ('810', '643')
            AND SUBSTRING(d.account_number, 1, 5) = p_ledger_account;

        SELECT COALESCE(SUM(credit_amount_rub), 0)
        INTO p_turn_cre_val
        FROM dm.dm_account_turnover_f AS da
        JOIN ds.md_account_d AS d ON da.account_rk = d.account_rk
        WHERE on_date BETWEEN p_from_date AND p_to_date
            AND currency_code NOT IN ('810', '643')
            AND SUBSTRING(d.account_number, 1, 5) = p_ledger_account;

        SELECT COALESCE(SUM(credit_amount_rub), 0)
        INTO p_turn_cre_total
        FROM dm.dm_account_turnover_f AS da
        JOIN ds.md_account_d AS d ON da.account_rk = d.account_rk
        WHERE on_date BETWEEN p_from_date AND p_to_date
            AND SUBSTRING(d.account_number, 1, 5) = p_ledger_account;

        SELECT COALESCE(SUM(balance_out_rub), 0)
        INTO p_balance_out_rub
        FROM dm.dm_account_balance_f AS da
        JOIN ds.md_account_d AS d ON da.account_rk = d.account_rk
        WHERE on_date = p_to_date
            AND currency_code IN ('810', '643')
            AND SUBSTRING(d.account_number, 1, 5) = p_ledger_account;

        SELECT COALESCE(SUM(balance_out_rub), 0)
        INTO p_balance_out_val
        FROM dm.dm_account_balance_f AS da
        JOIN ds.md_account_d AS d ON da.account_rk = d.account_rk
        WHERE on_date = p_to_date
            AND currency_code NOT IN ('810', '643')
            AND SUBSTRING(d.account_number, 1, 5) = p_ledger_account;

        SELECT COALESCE(SUM(balance_out_rub), 0)
        INTO p_balance_out_total
        FROM dm.dm_account_balance_f AS da
        JOIN ds.md_account_d AS d ON da.account_rk = d.account_rk
        WHERE on_date = p_to_date
            AND SUBSTRING(d.account_number, 1, 5) = p_ledger_account;
        
        INSERT INTO dm.dm_f101_round_f (
            from_date,
            to_date,
            chapter,
            ledger_account,
            characteristic, 
            balance_in_rub, 
            r_balance_in_rub, 
            balance_in_val, 
            r_balance_in_val, 
            balance_in_total, 
            r_balance_in_total, 
            turn_deb_rub, 
            r_turn_deb_rub, 
            turn_deb_val, 
            r_turn_deb_val, 
            turn_deb_total, 
            r_turn_deb_total, 
            turn_cre_rub, 
            r_turn_cre_rub, 
            turn_cre_val, 
            r_turn_cre_val, 
            turn_cre_total, 
            r_turn_cre_total, 
            balance_out_rub, 
            r_balance_out_rub, 
            balance_out_val, 
            r_balance_out_val, 
            balance_out_total,
            r_balance_out_total
        ) VALUES (
            p_from_date,
            p_to_date,
            p_chapter,
            p_ledger_account,
            p_characteristic,
            p_balance_in_rub,
            p_balance_in_rub / 1000,
            p_balance_in_val,
            p_balance_in_val / 1000,
            p_balance_in_total,
            p_balance_in_total / 1000,
            p_turn_deb_rub,
            p_turn_deb_rub / 1000,
            p_turn_deb_val,
            p_turn_deb_val / 1000,
            p_turn_deb_total,
            p_turn_deb_total / 1000,
            p_turn_cre_rub,
            p_turn_cre_rub / 1000,
            p_turn_cre_val,
            p_turn_cre_val / 1000,
            p_turn_cre_total,
            p_turn_cre_total / 1000,
            p_balance_out_rub,
            p_balance_out_rub / 1000,
            p_balance_out_val,
            p_balance_out_val / 1000,
            p_balance_out_total,
            p_balance_out_total / 1000
        );
    END LOOP;

    CLOSE accounts_cur;
END;
$$
