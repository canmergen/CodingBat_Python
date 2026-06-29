/* ================================================================
   KAMPANYA / WELCOME HAFTALIK TABLO — TURUNCU_YENI uzerinden.
   Haftada TEK satir: o hafta = haftanin SON IS GUNU snapshot'i.

   PERFORMANS (kritik):
   - TURUNCU_YENI TEK TARANIR. Son is gunu ayni gecmiste MAX()OVER ile secilir
     (ikinci tarama YOK).
   - osa_welcome RANGE join ARTIK ham satirda DEGIL; sadece kucuk (hafta,musteri)
     setinde yapilir (range-join patlamasi cozuldu).

   Kavramlar:
   - BONUS_*    = STANDART OSAWelcome. BONUS_BIT_TARIHI = welcome bitisi (HERKES).
   - KAMPANYA_* = EKSTRA kampanya (bazilarinda). KAMPANYA_BIT_TARIHI = ekstra bitisi.
   - "ekstra musteri" = KAMPANYA_ADI IS NOT NULL.

   HIZLI TEST: asagidaki iki "DATE '2024-09-16'" yerine yakin bir tarih
   (orn. DATE '2026-04-01') koyarsan birkac haftada calisip dogrular.
   Hafta: Pzt(16.09.2024) referans. Kolon adlarini gerekirse degistir.
   ================================================================ */
WITH
osa_welcome AS (
    SELECT t1.start_date, t1.end_date, t1.rate AS OSAWelcome
    FROM OPR.V_FS_SVIN_DEPOSIT_INTEREST t1
    WHERE t1.deleted = 0 AND t1.interest_code = 1 AND t1.currency_code = 0
),
/* 1) TEK TARAMA: hafta etiketi + o haftanin son is gunu (MAX OVER). Range join YOK. */
snap AS (
    SELECT /*+ MATERIALIZE */
        HAFTA_MIN, MUSTERI_NO, KAMPANYA_ADI,
        BONUS_BIT_TARIHI, KAMPANYA_BIT_TARIHI, KAMPANYA_FAIZ_ORANI
    FROM (
        SELECT /*+ PARALLEL(t, 4) */
            t.MUSTERI_NO,
            t.KAMPANYA_ADI,
            t.BONUS_BIT_TARIHI,
            t.KAMPANYA_BIT_TARIHI,
            t.KAMPANYA_FAIZ_ORANI,
            t.RAPOR_TARIHI,
            t.RAPOR_TARIHI - MOD(t.RAPOR_TARIHI - DATE '2024-09-16', 7) AS HAFTA_MIN,
            MAX(t.RAPOR_TARIHI) OVER (
                PARTITION BY t.RAPOR_TARIHI - MOD(t.RAPOR_TARIHI - DATE '2024-09-16', 7)
            ) AS WK_MAX
        FROM PRSN.TURUNCU_YENI t
        WHERE t.PARA_KODU = 0
          AND t.CALISMA_SEKLI = 1
          AND t.RAPOR_TARIHI >= DATE '2024-09-16'
          AND TO_CHAR(t.RAPOR_TARIHI, 'DY', 'NLS_DATE_LANGUAGE=AMERICAN') NOT IN ('SAT','SUN')
    )
    WHERE RAPOR_TARIHI = WK_MAX            -- sadece haftanin son is gunu
),
/* 2) (hafta, musteri) cokertme: ayni gun coklu hesap satirlarini tekille */
cust_week AS (
    SELECT /*+ MATERIALIZE */
        HAFTA_MIN,
        MUSTERI_NO,
        MAX(CASE WHEN KAMPANYA_ADI IS NOT NULL THEN 1 ELSE 0 END) AS KAMPANYALI,
        MAX(KAMPANYA_FAIZ_ORANI)                                  AS KAMPANYA_FAIZ_ORANI,
        MAX(BONUS_BIT_TARIHI)                                     AS BONUS_BIT_TARIHI,
        MAX(KAMPANYA_BIT_TARIHI)                                  AS KAMPANYA_BIT_TARIHI,
        MAX(KAMPANYA_ADI)                                         AS KAMPANYA_ADI
    FROM snap
    GROUP BY HAFTA_MIN, MUSTERI_NO
),
/* 3) hafta basina welcome orani (KUCUK range join: ~90 hafta) */
wk_welcome AS (
    SELECT wk.HAFTA_MIN, w.OSAWelcome
    FROM (SELECT DISTINCT HAFTA_MIN FROM cust_week) wk
    LEFT JOIN osa_welcome w ON wk.HAFTA_MIN BETWEEN w.start_date AND w.end_date
),
/* 4) welcome ustu bayragi (kucuk set uzerinde) */
cw AS (
    SELECT
        c.HAFTA_MIN, c.MUSTERI_NO, c.KAMPANYALI, c.KAMPANYA_ADI,
        c.BONUS_BIT_TARIHI, c.KAMPANYA_BIT_TARIHI,
        CASE WHEN c.KAMPANYA_FAIZ_ORANI > v.OSAWelcome + 0.0001 THEN 1 ELSE 0 END AS WELCOME_USTU
    FROM cust_week c
    LEFT JOIN wk_welcome v ON c.HAFTA_MIN = v.HAFTA_MIN
),
/* 5) hafta-level ozet */
wk_all AS (
    SELECT
        HAFTA_MIN,
        COUNT(*)                       AS toplam_musteri,
        SUM(KAMPANYALI)                AS kampanyali_musteri,
        SUM(1 - KAMPANYALI)            AS baseline_musteri,
        COUNT(DISTINCT KAMPANYA_ADI)   AS farkli_kampanya_sayisi,
        SUM(WELCOME_USTU)              AS welcome_ustu_musteri,
        SUM(CASE WHEN KAMPANYALI = 0
                  AND BONUS_BIT_TARIHI >= HAFTA_MIN     AND BONUS_BIT_TARIHI < HAFTA_MIN + 7
                 THEN 1 ELSE 0 END)    AS welcome_bitiyor_bu_hafta,
        SUM(CASE WHEN KAMPANYALI = 0
                  AND BONUS_BIT_TARIHI >= HAFTA_MIN + 7 AND BONUS_BIT_TARIHI < HAFTA_MIN + 14
                 THEN 1 ELSE 0 END)    AS welcome_bitiyor_gelecek_hafta,
        SUM(CASE WHEN KAMPANYALI = 1
                  AND KAMPANYA_BIT_TARIHI >= HAFTA_MIN     AND KAMPANYA_BIT_TARIHI < HAFTA_MIN + 7
                 THEN 1 ELSE 0 END)    AS kampanya_bitiyor_bu_hafta,
        SUM(CASE WHEN KAMPANYALI = 1
                  AND KAMPANYA_BIT_TARIHI >= HAFTA_MIN + 7 AND KAMPANYA_BIT_TARIHI < HAFTA_MIN + 14
                 THEN 1 ELSE 0 END)    AS kampanya_bitiyor_gelecek_hafta
    FROM cw
    GROUP BY HAFTA_MIN
),
/* 6) o haftanin en cok musterili kampanyasi */
wk_top AS (
    SELECT HAFTA_MIN,
           MAX(KAMPANYA_ADI) KEEP (DENSE_RANK LAST ORDER BY cnt, KAMPANYA_ADI) AS en_buyuk_kampanya,
           MAX(cnt)          KEEP (DENSE_RANK LAST ORDER BY cnt, KAMPANYA_ADI) AS en_buyuk_kampanya_musteri
    FROM (
        SELECT HAFTA_MIN, KAMPANYA_ADI, COUNT(*) AS cnt
        FROM cw
        WHERE KAMPANYALI = 1
        GROUP BY HAFTA_MIN, KAMPANYA_ADI
    )
    GROUP BY HAFTA_MIN
)
SELECT
    a.HAFTA_MIN                      AS MIN_HAFTA,
    a.HAFTA_MIN + 4                  AS MAX_HAFTA,
    a.toplam_musteri,
    a.kampanyali_musteri,
    a.baseline_musteri,
    a.farkli_kampanya_sayisi,
    t.en_buyuk_kampanya,
    t.en_buyuk_kampanya_musteri,
    a.welcome_ustu_musteri,
    a.welcome_bitiyor_bu_hafta,
    a.welcome_bitiyor_gelecek_hafta,
    a.kampanya_bitiyor_bu_hafta,
    a.kampanya_bitiyor_gelecek_hafta
FROM wk_all a
LEFT JOIN wk_top t ON a.HAFTA_MIN = t.HAFTA_MIN
ORDER BY a.HAFTA_MIN;
