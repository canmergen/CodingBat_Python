/* ================================================================
   KAMPANYA / WELCOME HAFTALIK TABLO — SADE.
   Haftada TEK satir = haftanin SON IS GUNU snapshot'i.
   Sadece HAM sayim + hacim uretir; oran / ort.bakiye / WoW degisim
   PYTHON'da (merge_campaign.py) hesaplanir.

   Kavramlar:
   - BONUS_*    = STANDART OSAWelcome (HERKES). BONUS_BIT_TARIHI = welcome bitisi.
   - KAMPANYA_* = EKSTRA kampanya (bazilarinda). BAS/BIT = ekstra basi/bitisi.
   - "ekstra musteri" = KAMPANYA_ADI IS NOT NULL.

   3 CTE: snapshot_days (son is gunu) -> cust_week (hafta,musteri) -> welcome (haftalik rate).
   Hafta: Pzt(16.09.2024) referans. Kolon adlarini gerekirse degistir.
   ================================================================ */
WITH
/* 1) Her haftanin SON IS GUNU — ucuz DISTINCT tarih (temp patlamaz) */
snapshot_days AS (
    SELECT RAPOR_TARIHI
    FROM (
        SELECT RAPOR_TARIHI,
               ROW_NUMBER() OVER (
                   PARTITION BY RAPOR_TARIHI - MOD(RAPOR_TARIHI - DATE '2024-09-16', 7)
                   ORDER BY RAPOR_TARIHI DESC) AS rn
        FROM (
            SELECT DISTINCT t.RAPOR_TARIHI
            FROM PRSN.TURUNCU_YENI t
            WHERE t.RAPOR_TARIHI >= DATE '2024-09-16'
              AND TO_CHAR(t.RAPOR_TARIHI, 'DY', 'NLS_DATE_LANGUAGE=AMERICAN') NOT IN ('SAT','SUN')
        )
    )
    WHERE rn = 1
),
/* 2) Snapshot gunleri -> (hafta, musteri): ayni gun coklu hesap satirlari tekillenir */
cust_week AS (
    SELECT
        t.RAPOR_TARIHI - MOD(t.RAPOR_TARIHI - DATE '2024-09-16', 7) AS HAFTA_MIN,
        t.MUSTERI_NO,
        MAX(CASE WHEN t.KAMPANYA_ADI IS NOT NULL THEN 1 ELSE 0 END) AS KAMPANYALI,
        MAX(t.KAMPANYA_FAIZ_ORANI)  AS KAMPANYA_FAIZ_ORANI,
        MAX(t.BONUS_BIT_TARIHI)     AS BONUS_BIT_TARIHI,
        MAX(t.KAMPANYA_BAS_TARIHI)  AS KAMPANYA_BAS_TARIHI,
        MAX(t.KAMPANYA_BIT_TARIHI)  AS KAMPANYA_BIT_TARIHI,
        MAX(t.KAMPANYA_ADI)         AS KAMPANYA_ADI,
        SUM(t.TH_TOTAL_BAKIYE)      AS MUSTERI_HACIM
    FROM PRSN.TURUNCU_YENI t
    JOIN snapshot_days s ON t.RAPOR_TARIHI = s.RAPOR_TARIHI
    WHERE t.PARA_KODU = 0 AND t.CALISMA_SEKLI = 1
    GROUP BY t.RAPOR_TARIHI - MOD(t.RAPOR_TARIHI - DATE '2024-09-16', 7), t.MUSTERI_NO
),
/* 3) Haftalik standart welcome orani (kucuk range join) */
welcome AS (
    SELECT wk.HAFTA_MIN, w.rate AS OSAWelcome
    FROM (SELECT DISTINCT HAFTA_MIN FROM cust_week) wk
    LEFT JOIN OPR.V_FS_SVIN_DEPOSIT_INTEREST w
           ON w.deleted = 0 AND w.interest_code = 1 AND w.currency_code = 0
          AND wk.HAFTA_MIN BETWEEN w.start_date AND w.end_date
)
/* -- HAM haftalik ozet (oran/ort.bakiye/degisim PYTHON'da) -- */
SELECT
    c.HAFTA_MIN                                                       AS MIN_HAFTA,
    c.HAFTA_MIN + 4                                                   AS MAX_HAFTA,
    /* sayi */
    COUNT(*)                                                         AS toplam_musteri,
    SUM(c.KAMPANYALI)                                               AS kampanyali_musteri,
    SUM(1 - c.KAMPANYALI)                                           AS baseline_musteri,
    SUM(CASE WHEN c.KAMPANYA_FAIZ_ORANI > w.OSAWelcome + 0.0001 THEN 1 ELSE 0 END) AS welcome_ustu_musteri,
    COUNT(DISTINCT c.KAMPANYA_ADI)                                 AS farkli_kampanya_sayisi,
    /* hacim (TH_TOTAL_BAKIYE) */
    SUM(c.MUSTERI_HACIM)                                            AS toplam_hacim,
    SUM(CASE WHEN c.KAMPANYALI = 1 THEN c.MUSTERI_HACIM ELSE 0 END) AS kampanyali_hacim,
    SUM(CASE WHEN c.KAMPANYALI = 0 THEN c.MUSTERI_HACIM ELSE 0 END) AS baseline_hacim,
    SUM(CASE WHEN c.KAMPANYA_FAIZ_ORANI > w.OSAWelcome + 0.0001 THEN c.MUSTERI_HACIM ELSE 0 END) AS welcome_ustu_hacim,
    /* kampanya rate ortalamasi + welcome primi (kampanyalilarda) */
    ROUND(AVG(CASE WHEN c.KAMPANYALI = 1 THEN c.KAMPANYA_FAIZ_ORANI END), 2)              AS ort_kampanya_rate,
    ROUND(AVG(CASE WHEN c.KAMPANYALI = 1 THEN c.KAMPANYA_FAIZ_ORANI - w.OSAWelcome END), 2) AS kampanya_primi,
    /* welcome (bonus) bitisi — baseline outflow oncusu: adet + hacim */
    SUM(CASE WHEN c.KAMPANYALI = 0 AND c.BONUS_BIT_TARIHI >= c.HAFTA_MIN     AND c.BONUS_BIT_TARIHI < c.HAFTA_MIN + 7  THEN 1 ELSE 0 END)            AS welcome_bitiyor_bu_hafta,
    SUM(CASE WHEN c.KAMPANYALI = 0 AND c.BONUS_BIT_TARIHI >= c.HAFTA_MIN + 7 AND c.BONUS_BIT_TARIHI < c.HAFTA_MIN + 14 THEN 1 ELSE 0 END)            AS welcome_bitiyor_gelecek_hafta,
    SUM(CASE WHEN c.KAMPANYALI = 0 AND c.BONUS_BIT_TARIHI >= c.HAFTA_MIN     AND c.BONUS_BIT_TARIHI < c.HAFTA_MIN + 7  THEN c.MUSTERI_HACIM ELSE 0 END) AS welcome_bitiyor_bu_hafta_hacim,
    SUM(CASE WHEN c.KAMPANYALI = 0 AND c.BONUS_BIT_TARIHI >= c.HAFTA_MIN + 7 AND c.BONUS_BIT_TARIHI < c.HAFTA_MIN + 14 THEN c.MUSTERI_HACIM ELSE 0 END) AS welcome_bitiyor_gelecek_hafta_hacim,
    /* kampanya bitisi — gelecek hafta (aktifken yakalanir): adet + hacim */
    SUM(CASE WHEN c.KAMPANYALI = 1 AND c.KAMPANYA_BIT_TARIHI >= c.HAFTA_MIN + 7 AND c.KAMPANYA_BIT_TARIHI < c.HAFTA_MIN + 14 THEN 1 ELSE 0 END)            AS kampanya_bitiyor_gelecek_hafta,
    SUM(CASE WHEN c.KAMPANYALI = 1 AND c.KAMPANYA_BIT_TARIHI >= c.HAFTA_MIN + 7 AND c.KAMPANYA_BIT_TARIHI < c.HAFTA_MIN + 14 THEN c.MUSTERI_HACIM ELSE 0 END) AS kampanya_bitiyor_gelecek_hafta_hacim,
    /* kampanya basliyor — bu hafta (basladi, aktif): adet + hacim */
    SUM(CASE WHEN c.KAMPANYALI = 1 AND c.KAMPANYA_BAS_TARIHI >= c.HAFTA_MIN AND c.KAMPANYA_BAS_TARIHI < c.HAFTA_MIN + 7 THEN 1 ELSE 0 END)            AS kampanya_basliyor_bu_hafta,
    SUM(CASE WHEN c.KAMPANYALI = 1 AND c.KAMPANYA_BAS_TARIHI >= c.HAFTA_MIN AND c.KAMPANYA_BAS_TARIHI < c.HAFTA_MIN + 7 THEN c.MUSTERI_HACIM ELSE 0 END) AS kampanya_basliyor_bu_hafta_hacim
FROM cust_week c
LEFT JOIN welcome w ON c.HAFTA_MIN = w.HAFTA_MIN
GROUP BY c.HAFTA_MIN
ORDER BY c.HAFTA_MIN;
