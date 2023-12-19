/*
ETL Pipeline for DNB Matches
For business decision and limitation of the API we cannot apply the same logic for everycountry some countries still keep the old engine
Note: Some columns were obfuscated 
*/
SELECT raw1.bu_account_key                   AS bu_account_key,
       raw1.bu_account_id                    AS bu_account_id,
       raw1.erp_id                           AS erp_id,
       raw1.bu_secondary_key                 AS bu_secondary_key,
       raw1.db_company_id                    AS db_company_id,
       raw1.account_name                     AS company_name,
       CASE WHEN raw1.street ='.'
            THEN NULL
            ELSE raw1.street
        END                                   AS street_address,
       CASE WHEN raw1.po_box = '?'
            THEN NULL
            ELSE raw1.po_box
        END                                   AS po_box,
       raw1.city                              AS primary_city,
       raw1.state                             AS primary_state,
       raw1.zip_code                          AS primary_zip,
       raw1.country                           AS primary_country,
       raw1.tel_number                        AS primary_phone,
       CASE WHEN DUNS_NUMBER = '999999999'
            THEN NULL
            ELSE raw1.duns_number
        END                                   AS duns_number,
       raw1.duns_match_confidence_codes       AS duns_match_confidence_codes,
       raw1.source                            AS source,
       CASE WHEN (raw1.tax_id ='?' OR raw1.tax_id IS NULL)
            THEN NULL
            WHEN LENGTH(raw1.tax_id) >=15  AND LOWER(raw1.country) LIKE '%china%'
            THEN raw1.tax_id
            WHEN LENGTH(raw1.tax_id) >=13  AND LOWER(raw1.country) LIKE '%korea%'
            THEN raw1.tax_id
            WHEN LENGTH(raw1.tax_id) >=10  AND LOWER(raw1.country) LIKE '%taiwan%'
            THEN raw1.tax_id
            WHEN LENGTH(raw1.tax_id) >=12  AND LOWER(raw1.country) LIKE '%japan%'
            THEN raw1.tax_id
            ELSE NULL
        END                    AS tax_id,
       CASE WHEN LOWER(raw1.country) LIKE '%china%'
            THEN 'China'
            WHEN LOWER(raw1.country) LIKE '%korea%'
            THEN 'Korea'
            WHEN LOWER(raw1.country) LIKE '%taiwan%'
            THEN 'Taiwan'
            WHEN LOWER(raw1.country) LIKE '%japan%'
            THEN 'Japan'
        END                    AS country
FROM accounts_master  raw1
WHERE raw1.bu_account_id IS NOT NULL
 AND NVL(raw1.duns_number,'999999999') ='999999999'
 AND LENGTH(raw1.account_name) != LENGTHB(raw1.account_name)
 AND duns_match_confidence_codes < 100
 AND ( LOWER(raw1.country) LIKE '%china%' OR
       LOWER(raw1.country) LIKE '%korea%' OR
       LOWER(raw1.country) LIKE '%japan%' OR
       LOWER(raw1.country) LIKE '%taiwan%'
    );
TYPE typ_dnb_match_recert_ame IS TABLE OF cur_dnb_match_recert_ame%ROWTYPE;
v_dnb_match_recert_ame typ_dnb_match_recert_ame  := typ_dnb_match_recert_ame();
v_count           NUMBER;
v_total_count     NUMBER;
v_cnt             NUMBER;
BEGIN
v_total_count    := 0;
v_count          := 0;
v_cnt            := 0;
EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT=''DD-MON-YYYY HH24:MI:SS''';
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ame_dnb_match_recert_acc';
EXCEPTION
    WHEN OTHERS
    THEN
        NULL;
END;
OPEN cur_dnb_match_recert_ame;
LOOP
    FETCH cur_dnb_match_recert_ame
     BULK COLLECT
     INTO v_dnb_match_recert_ame
    LIMIT 1000;
    EXIT WHEN v_dnb_match_recert_ame.COUNT = 0;

    FORALL i IN v_dnb_match_recert_ame.FIRST .. v_dnb_match_recert_ame.LAST
        INSERT
          INTO ame_dnb_match_recert_acc
              (bu_account_key,
               bu_account_id,
               erp_id,
               bu_secondary_key,
               db_company_id,
               company_name,
               street_address,
               po_box,
               primary_city,
               primary_state,
               primary_zip,
               primary_country,
               primary_phone,
               duns_number,
               duns_match_confidence_codes,
               source,
               tax_id,
               country
              )
        VALUES(v_dnb_match_recert_ame(i).bu_account_key,
               v_dnb_match_recert_ame(i).bu_account_id,
               v_dnb_match_recert_ame(i).erp_id,
               v_dnb_match_recert_ame(i).bu_secondary_key,
               v_dnb_match_recert_ame(i).db_company_id,
               v_dnb_match_recert_ame(i).company_name,
               v_dnb_match_recert_ame(i).street_address,
               v_dnb_match_recert_ame(i).po_box,
               v_dnb_match_recert_ame(i).primary_city,
               v_dnb_match_recert_ame(i).primary_state,
               v_dnb_match_recert_ame(i).primary_zip,
               v_dnb_match_recert_ame(i).primary_country,
               v_dnb_match_recert_ame(i).primary_phone,
               v_dnb_match_recert_ame(i).duns_number,
               v_dnb_match_recert_ame(i).duns_match_confidence_codes,
               v_dnb_match_recert_ame(i).source,
               v_dnb_match_recert_ame(i).tax_id,
               v_dnb_match_recert_ame(i).country
              );
                       COMMIT;
        v_count :=  v_count + v_dnb_match_recert_ame.count;
        v_dnb_match_recert_ame.DELETE;
END LOOP;
CLOSE cur_dnb_match_recert_ame;
END prc_dnb_match_recert_ame;