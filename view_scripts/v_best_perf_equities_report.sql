-- View: public.v_best_perf_equities_report

-- DROP VIEW public.v_best_perf_equities_report;

CREATE OR REPLACE VIEW public.v_best_perf_equities_report
 AS
 SELECT mnt_top_fund.financial_type,
    mnt_top_fund.file_dt,
    cumulative.fund_nm,
    mnt_top_fund.realised_p_l AS best_fund_of_month,
    cumulative.cumulative_net AS cumulative_p_l
   FROM ( WITH cte AS (
                 SELECT investment_funds.fund_nm,
                    investment_funds.file_dt,
                    to_char(date_trunc('month'::text, investment_funds.file_dt::date::timestamp with time zone), 'YYYY-MM'::text) AS month,
                    sum(
                        CASE
                            WHEN investment_funds.realised_p_l > 0::numeric THEN investment_funds.realised_p_l
                            ELSE 0::numeric
                        END) AS total_profit,
                    sum(
                        CASE
                            WHEN investment_funds.realised_p_l < 0::numeric THEN investment_funds.realised_p_l
                            ELSE 0::numeric
                        END) AS total_loss,
                    sum(investment_funds.realised_p_l) AS profit_or_loss
                   FROM investment_funds
                  WHERE investment_funds.financial_type::text = 'Equities'::text
                  GROUP BY investment_funds.fund_nm, investment_funds.file_dt, (to_char(date_trunc('month'::text, investment_funds.file_dt::date::timestamp with time zone), 'YYYY-MM'::text))
                  ORDER BY investment_funds.fund_nm, investment_funds.file_dt
                )
         SELECT cte.fund_nm,
            cte.month,
            sum(cte.total_profit) OVER (PARTITION BY cte.fund_nm ORDER BY cte.month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_profit,
            sum(cte.total_loss) OVER (PARTITION BY cte.fund_nm ORDER BY cte.month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_loss,
            sum(cte.profit_or_loss) OVER (PARTITION BY cte.fund_nm ORDER BY cte.month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_net
           FROM cte) cumulative
     JOIN ( SELECT temp.financial_type,
            temp.fund_nm,
            temp.file_dt,
            temp.realised_p_l,
            temp.rnk,
            temp.month
           FROM ( SELECT investment_funds.financial_type,
                    investment_funds.fund_nm,
                    investment_funds.file_dt,
                    sum(investment_funds.realised_p_l) AS realised_p_l,
                    rank() OVER (PARTITION BY investment_funds.file_dt ORDER BY (sum(investment_funds.realised_p_l)) DESC) AS rnk,
                    to_char(date_trunc('month'::text, investment_funds.file_dt::date::timestamp with time zone), 'YYYY-MM'::text) AS month
                   FROM investment_funds
                  WHERE investment_funds.financial_type::text = 'Equities'::text
                  GROUP BY investment_funds.financial_type, investment_funds.fund_nm, investment_funds.file_dt) temp
          WHERE temp.rnk = 1) mnt_top_fund ON cumulative.fund_nm::text = mnt_top_fund.fund_nm::text AND cumulative.month = mnt_top_fund.month
  ORDER BY cumulative.month;

ALTER TABLE public.v_best_perf_equities_report
    OWNER TO postgres;

