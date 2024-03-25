-- View: public.v_equities_bond_price_recon

-- DROP VIEW public.v_equities_bond_price_recon;

CREATE OR REPLACE VIEW public.v_equities_bond_price_recon
 AS
 SELECT funds.financial_type,
    funds.fund_nm,
    funds.file_dt::date AS file_dt,
    sum(funds.instrument_price) AS instrument_price,
    sum(ref.reference_price) AS reference_price,
    sum((funds.instrument_price::double precision - ref.reference_price)::numeric(10,5)) AS price_diff
   FROM ( SELECT investment_funds.financial_type,
            investment_funds.symbol,
            investment_funds.security_name,
            investment_funds.price AS instrument_price,
            investment_funds.fund_nm,
            investment_funds.file_dt
           FROM investment_funds
          WHERE investment_funds.financial_type::text = 'Equities'::text) funds
     LEFT JOIN ( SELECT eref."SYMBOL",
            eref."COUNTRY",
            eref."SECURITY NAME",
            eref."SECTOR",
            eref."INDUSTRY",
            eref."CURRENCY",
            ep."DATETIME"::date AS datetime,
            ep."PRICE" AS reference_price
           FROM equity_reference eref
             JOIN equity_prices ep ON eref."SYMBOL" = ep."SYMBOL"
          WHERE ep."DATETIME"::date = (date_trunc('month'::text, ep."DATETIME"::date::timestamp with time zone) + '1 mon -1 days'::interval)::date) ref ON funds.security_name::text = ref."SECURITY NAME" AND funds.symbol::text = ref."SYMBOL" AND funds.file_dt::date = ref.datetime
  GROUP BY funds.financial_type, funds.fund_nm, (funds.file_dt::date)
UNION
 SELECT invfund.financial_type,
    invfund.fund_nm,
    invfund.file_dt::date AS file_dt,
    sum(invfund.instrument_price) AS instrument_price,
    sum(bonref.reference_price) AS reference_price,
    sum((invfund.instrument_price::double precision - bonref.reference_price)::numeric(10,5)) AS price_diff
   FROM ( SELECT bref."SECURITY NAME",
            bref."ISIN",
            bp."DATETIME",
            bp."PRICE" AS reference_price,
            eom_dt.month,
            eom_dt.max_date
           FROM bond_reference bref
             JOIN bond_prices bp ON bref."ISIN" = bp."ISIN"
             JOIN ( SELECT to_char(date_trunc('month'::text, bond_prices."DATETIME"::date::timestamp with time zone), 'YYYY-MM'::text) AS month,
                    max(bond_prices."DATETIME"::date) AS max_date
                   FROM bond_prices
                  GROUP BY (to_char(date_trunc('month'::text, bond_prices."DATETIME"::date::timestamp with time zone), 'YYYY-MM'::text))) eom_dt ON bp."DATETIME"::date = eom_dt.max_date) bonref
     JOIN ( SELECT investment_funds.financial_type,
            investment_funds.symbol,
            investment_funds.security_name,
            investment_funds.price AS instrument_price,
            investment_funds.fund_nm,
            investment_funds.file_dt,
            to_char(date_trunc('month'::text, investment_funds.file_dt::date::timestamp with time zone), 'YYYY-MM'::text) AS month
           FROM investment_funds
          WHERE investment_funds.financial_type::text = 'Government Bond'::text) invfund ON bonref."SECURITY NAME" = invfund.security_name::text AND bonref."ISIN" = invfund.symbol::text AND bonref.month = invfund.month
  GROUP BY invfund.financial_type, invfund.fund_nm, (invfund.file_dt::date);

ALTER TABLE public.v_equities_bond_price_recon
    OWNER TO postgres;

