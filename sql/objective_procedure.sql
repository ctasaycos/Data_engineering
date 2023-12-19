/*
AOP (Annual Objective Proposal)
Here we are pulling data to generate our AOP for the corporate accounts based on segments, territories, divisions and so on.
The logic was set up by the stakeholders (Finance Team)
Some fields were obfuscated for privacy
*/
WITH data AS(SELECT level lvl
                       FROM dual
                    CONNECT BY LEVEL <= 12
                    )
        SELECT '2023'                      AS aop_year,
               lvl                         AS month,
               aop.duns                    AS gu_duns,
               duns_acc.account_name       AS customer,
               duns_acc.ca_segment      AS ca_segment,
               duns_acc.ca_territory    AS ca_territory,
               duns_acc.segment            AS segment,
               duns_acc.sub_segment     AS sub_segment,
               duns_acc.rpt_grp1           AS rpt_grp1,
               duns_acc.rpt_grp2           AS rpt_grp2,
               duns_acc.rpt_grp3           AS rpt_grp3,
               duns_acc.ca_flag         AS ca_flag,
               duns_acc.corp_strategy      AS corp_strategy,
               aop.division                AS division,
               aop.sub_division            AS sub_division,
               aop.product_exclusion       AS product_exclusion,
               'Oppty'                     AS aop_type,
               CASE WHEN lvl = 1
                    THEN
                       (q1_total *0.34)
                    WHEN lvl = 2
                    THEN
                       (q1_total *0.32)
                    WHEN lvl = 3
                    THEN
                        (q1_total *0.34)
                    WHEN lvl = 4
                    THEN
                        (q2_total *0.34)
                    WHEN lvl = 5
                    THEN
                        (q2_total *0.32)
                    WHEN lvl = 6
                    THEN
                        (q2_total *0.34)
                    WHEN lvl = 7
                    THEN
                        (q3_total *0.34)
                    WHEN lvl = 8
                    THEN
                        (q3_total *0.32)
                    WHEN lvl = 9
                    THEN
                        (q3_total *0.34)
                    WHEN lvl = 10
                    THEN
                        (q4_total *0.34)
                    WHEN lvl = 11
                    THEN
                        (q4_total *0.32)
                    WHEN lvl = 12
                    THEN
                        (q4_total *0.34)
                 END AS aop_amount
         FROM obf.aop_opportunity_2023 aop,
              obf.accounts duns_acc,
              data
        WHERE duns_acc.parent_duns = aop.duns
          AND duns_acc.ca_segment IN ('obfusc')             
        ORDER by aop_opportunity_id,lvl;
        TYPE typ_aop_oppty_2023 IS TABLE OF cur_aop_oppty_2023%ROWTYPE;
        v_typ_aop_oppty_2023 typ_aop_oppty_2023  := typ_aop_oppty_2023();
    BEGIN
        EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT=''DD-MON-YYYY HH24:MI:SS''';
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE obf_table';
        EXCEPTION
            WHEN OTHERS
            THEN
                NULL;
        END;