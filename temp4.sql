/* ================================================================
   KAMPANYA / WELCOME HAFTALIK TABLO — TURUNCU_YENI uzerinden.
   Haftada TEK satir: o hafta ne olmus.

   SEMANTIK: her hafta = haftanin SON IS GUNU snapshot'i (osabook/netflow
   ile ayni baz). "hafta ici herhangi gun aktif" DEGIL, "son is gunu aktif".
   Tatil/eksik gunde literal Cuma yerine o haftanin mevcut son is gunu alinir.

   Kavramlar (kullanici teyidi):
   - BONUS_*    = STANDART OSAWelcome (HERKESTE). BONUS_BIT_TARIHI = welcome bitisi.
   - KAMPANYA_* = bazi musterilere EKSTRA kampanya. KAMPANYA_BIT_TARIHI = ekstranin bitisi.
   - "ekstra musteri" = KAMPANYA_ADI IS NOT NULL.

   HIZLANDIRMA:
   - snapshot_days: ucuz DISTINCT RAPOR_TARIHI ile her haftanin son is gunu (~90 gun).
   - Buyuk tablo SADECE bu gunlere filtrelenir -> okunan satir ~5 kat azalir.
   - Hafta etiketi tarih aritmetigiyle (range join yok), tek MATERIALIZE.

   Hafta: Pzt(16.09.2024) referans. Kolon adlarini gerekirse degistir.
   ================================================================ */
WITH
osa_welcome AS (
    SELECT t1.start_date, t1.end_date, t1.rate AS OSAWelcome
    FROM OPR.V_FS_SVIN_DEPOSIT_INTEREST t1
    WHERE t1.deleted = 0 AND t1.interest_code = 1 AND t1.currency_code = 0
),
/* 0) Her haftanin SON IS GUNU (tatil/eksik gun robust). Ucuz: sadece tarih kolonu. */
snapshot_days AS (
    SELECT /*+ MATERIALIZE */ RAPOR_TARIHI
    FROM (
        SELECT RAPOR_TARIHI,
               ROW_NUMBER() OVER (
                   PARTITION BY RAPOR_TARIHI - MOD(RAPOR_TARIHI - DATE '2024-09-16', 7)
                   ORDER BY RAPOR_TARIHI DESC) AS rn
        FROM (
            SELECT DISTINCT RAPOR_TARIHI
            FROM PRSN.TURUNCU_YENI
            WHERE RAPOR_TARIHI >= DATE '2024-09-16'
              AND TO_CHAR(RAPOR_TARIHI, 'DY', 'NLS_DATE_LANGUAGE=AMERICAN') NOT IN ('SAT','SUN')
        )
    )
    WHERE rn = 1
),
/* 1) Sadece snapshot gunleri: aktif TL musteri + hafta etiketi + welcome ustu bayragi */
daily AS (
    SELECT /*+ MATERIALIZE */
        t.RAPOR_TARIHI - MOD(t.RAPOR_TARIHI - DATE '2024-09-16', 7) AS HAFTA_MIN,
        t.MUSTERI_NO,
        t.KAMPANYA_ADI,
        t.BONUS_BIT_TARIHI,
        t.KAMPANYA_BIT_TARIHI,
        CASE WHEN t.KAMPANYA_ADI IS NOT NULL THEN 1 ELSE 0 END                    AS KAMPANYALI,
        CASE WHEN t.KAMPANYA_FAIZ_ORANI > w.OSAWelcome + 0.0001 THEN 1 ELSE 0 END AS WELCOME_USTU
    FROM PRSN.TURUNCU_YENI t
    JOIN snapshot_days s ON t.RAPOR_TARIHI = s.RAPOR_TARIHI
    LEFT JOIN osa_welcome w ON t.RAPOR_TARIHI BETWEEN w.start_date AND w.end_date
    WHERE t.PARA_KODU = 0
      AND t.CALISMA_SEKLI = 1
),
/* 2) (hafta, musteri) cokertme: ayni gun coklu hesap satirlarini tekille */
cust_week AS (
    SELECT /*+ MATERIALIZE */
        HAFTA_MIN,
        MUSTERI_NO,
        MAX(KAMPANYALI)          AS KAMPANYALI,
        MAX(WELCOME_USTU)        AS WELCOME_USTU,
        MAX(BONUS_BIT_TARIHI)    AS BONUS_BIT_TARIHI,
        MAX(KAMPANYA_BIT_TARIHI) AS KAMPANYA_BIT_TARIHI,
        MAX(KAMPANYA_ADI)        AS KAMPANYA_ADI
    FROM daily
    GROUP BY HAFTA_MIN, MUSTERI_NO
),
/* 3) hafta-level ozet (COUNT(*) = distinct musteri) */
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
    FROM cust_week
    GROUP BY HAFTA_MIN
),
/* 4) o haftanin en cok musterili kampanyasi */
wk_top AS (
    SELECT HAFTA_MIN,
           MAX(KAMPANYA_ADI) KEEP (DENSE_RANK LAST ORDER BY cnt, KAMPANYA_ADI) AS en_buyuk_kampanya,
           MAX(cnt)          KEEP (DENSE_RANK LAST ORDER BY cnt, KAMPANYA_ADI) AS en_buyuk_kampanya_musteri
    FROM (
        SELECT HAFTA_MIN, KAMPANYA_ADI, COUNT(*) AS cnt
        FROM cust_week
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
