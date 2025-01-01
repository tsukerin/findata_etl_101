CREATE OR REPLACE PROCEDURE "DS".fill_account_turnover_f (i_ondate DATE) 
LANGUAGE plpgsql
AS 
$$
DECLARE 
    curr_account_rk INTEGER;
    credit_amount NUMERIC(23, 8);
    credit_amount_rub NUMERIC(23, 8);
    debet_amount NUMERIC(23, 8);
    debet_amount_rub NUMERIC(23, 8);
BEGIN
    -- Расчет идентификатора счета
    SELECT "ACCOUNT_RK"
    INTO curr_account_rk
    FROM "DS"."FT_BALANCE_F"
    WHERE "ON_DATE" = i_ondate;

    -- Расчет суммы проводок за дату расчета
    SELECT "CREDIT_AMOUNT"
    INTO credit_amount
    FROM 
        

    INSERT INTO "DM"."DM_ACCOUNT_TURNOVER_F" VALUES (
        i_ondate,
        curr_account_rk,

    )
END;
$$