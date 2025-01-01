CREATE OR REPLACE PROCEDURE "DS".fill_account_turnover_f (i_ondate DATE) 
LANGUAGE plpgsql
AS 
$$
DECLARE 
    curr_account_rk INTEGER;
    credit_amount NUMERIC(23, 8) := 0;
    credit_amount_rub NUMERIC(23, 8) := 0;
    debet_amount NUMERIC(23, 8) := 0;
    debet_amount_rub NUMERIC(23, 8) := 0;
    actual_course FLOAT := 1;
BEGIN
    -- Находим идентификатор счета
    SELECT COALESCE("CREDIT_ACCOUNT_RK", 0)
    INTO curr_account_rk
    FROM "DS"."FT_POSTING_F"
    WHERE "OPER_DATE" = i_ondate;

    -- Находим сумму проводок за дату расчета для CREDIT
    SELECT COALESCE("CREDIT_AMOUNT", 0)
    INTO credit_amount
    FROM "DS"."FT_POSTING_F"
    WHERE "OPER_DATE" = i_ondate;

    -- Находим актуальный курс
    SELECT COALESCE("REDUCED_COURCE", 1)
    INTO actual_course
    FROM "DS"."MD_EXCHANGE_RATE_D" 
    WHERE i_ondate BETWEEN "DATA_ACTUAL_DATE" AND "DATA_ACTUAL_END_DATE";

    -- Находим сумму проводок за дату расчета в рублях для CREDIT
    SELECT COALESCE("CREDIT_AMOUNT" * actual_course, 0)
    INTO credit_amount_rub
    FROM "DS"."FT_POSTING_F"
    WHERE "OPER_DATE" = i_ondate;

    -- Проделываем то же самое для DEBET
    SELECT COALESCE("DEBET_AMOUNT", 0)
    INTO debet_amount
    FROM "DS"."FT_POSTING_F"
    WHERE "OPER_DATE" = i_ondate;

    SELECT COALESCE("DEBET_AMOUNT" * actual_course, 0)
    INTO debet_amount_rub
    FROM "DS"."FT_POSTING_F"
    WHERE "OPER_DATE" = i_ondate;

    -- Вставляем данные в итоговую таблицу
    INSERT INTO "DM"."DM_ACCOUNT_TURNOVER_F" VALUES (
        i_ondate,
        curr_account_rk,
        credit_amount,
        credit_amount_rub,
        debet_amount,
        debet_amount_rub
    );
END;
$$;
