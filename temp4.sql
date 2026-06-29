/* ================================================================
   KAMPANYA / WELCOME BITIS ANALIZI — TURUNCU_YENI uzerinden.

   IKI AYRI KAVRAM (kullanici teyidi):
   - BONUS_*  = STANDART OSAWelcome.  BONUS_BAS_TARIHI = welcome basi
                (= hesap acilis), BONUS_BIT_TARIHI = welcome bitisi (HERKESTE).
   - KAMPANYA_* = bazi musterilere tanimli EKSTRA kampanya.
                KAMPANYA_BIT_TARIHI = ekstranin bitisi (BAZILARINDA).

   => "ekstra/ustu-welcome musteri" = KAMPANYA tanimli olan
      (KAMPANYA_ADI IS NOT NULL). pure_osawelcome filtresinin temiz hali.

   Feature mantigi:
   - bonus_bitiyor_*     : baseline musterilerin welcome'i bitiyor -> repricing/outflow baskisi (BASELINE sinyali)
   - kampanya_bitiyor_*  : ekstra kampanya bitiyor -> ekstra musteri outflow (OVERLAY sinyali)

   Hafta kurgusu netflow_weekly ile ayni: Pzt(16.09.2024)-Cuma.
   Kolon adlarini gerekirse gercek adlariyla degistir.
   ================================================================

   ----------------------------------------------------------------
   SORGU A — Haftalik x KAMPANYA_ADI musteri dagilimi
   (hangi hafta hangi kampanyada kac musteri + ortalama rate)
   ---------------------------------------------------------------- */
WITH
CTE_WEEKS AS (
    SELECT
        DATE '2024-09-16' + (LEVEL - 1) * 7       AS HAFTA_MIN,
        DATE '2024-09-16' + (LEVEL - 1) * 7 + 4   AS HAFTA_MAX
    FROM DUAL
    CONNECT BY LEVEL - 1 <= FLOOR((TRUNC(SYSDATE) - DATE '2024-09-16') / 7)
),
osa_welcome AS (
    SELECT t1.start_date, t1.end_date, t1.rate AS OSAWelcome
    FROM OPR.V_FS_SVIN_DEPOSIT_INTEREST t1
    WHERE t1.deleted = 0 AND t1.interest_code = 1 AND t1.currency_code = 0
),
base_kmp AS (
    SELECT DISTINCT
        t.RAPOR_TARIHI, t.MUSTERI_NO, t.KAMPANYA_ADI, t.KAMPANYA_FAIZ_ORANI,
        t.KAMPANYA_BAS_TARIHI, t.KAMPANYA_BIT_TARIHI,
        CASE WHEN t.KAMPANYA_FAIZ_ORANI > w.OSAWelcome + 0.0001 THEN 1 ELSE 0 END AS WELCOME_USTU
    FROM PRSN.TURUNCU_YENI t
    LEFT JOIN osa_welcome w ON t.RAPOR_TARIHI BETWEEN w.start_date AND w.end_date
    WHERE t.PARA_KODU = 0 AND t.CALISMA_SEKLI = 1
      AND t.RAPOR_TARIHI >= DATE '2024-09-16'
      AND t.KAMPANYA_ADI IS NOT NULL
)
SELECT
    w.HAFTA_MIN, w.HAFTA_MAX,
    b.KAMPANYA_ADI,
    COUNT(DISTINCT b.MUSTERI_NO)                                       AS musteri_sayisi,
    ROUND(AVG(b.KAMPANYA_FAIZ_ORANI), 2)                              AS ort_kampanya_rate,
    COUNT(DISTINCT CASE WHEN b.WELCOME_USTU = 1 THEN b.MUSTERI_NO END) AS welcome_ustu_musteri
FROM CTE_WEEKS w
JOIN base_kmp b ON b.RAPOR_TARIHI BETWEEN w.HAFTA_MIN AND w.HAFTA_MAX
GROUP BY w.HAFTA_MIN, w.HAFTA_MAX, b.KAMPANYA_ADI
ORDER BY w.HAFTA_MIN, musteri_sayisi DESC;


/* ================================================================
   SORGU B — Haftalik OZET: welcome(bonus) bitisi + kampanya bitisi
   (modele eklenebilir feature'lar)
   ================================================================ */
WITH
CTE_WEEKS AS (
    SELECT
        DATE '2024-09-16' + (LEVEL - 1) * 7       AS HAFTA_MIN,
        DATE '2024-09-16' + (LEVEL - 1) * 7 + 4   AS HAFTA_MAX
    FROM DUAL
    CONNECT BY LEVEL - 1 <= FLOOR((TRUNC(SYSDATE) - DATE '2024-09-16') / 7)
),
/* tum aktif TL musteri-gun + bonus/kampanya bitis tarihleri + kampanyali bayragi */
base AS (
    SELECT DISTINCT
        t.RAPOR_TARIHI,
        t.MUSTERI_NO,
        t.BONUS_BIT_TARIHI,
        t.KAMPANYA_BIT_TARIHI,
        CASE WHEN t.KAMPANYA_ADI IS NOT NULL THEN 1 ELSE 0 END AS KAMPANYALI
    FROM PRSN.TURUNCU_YENI t
    WHERE t.PARA_KODU = 0 AND t.CALISMA_SEKLI = 1
      AND t.RAPOR_TARIHI >= DATE '2024-09-16'
)
SELECT
    w.HAFTA_MIN,
    w.HAFTA_MAX,
    COUNT(DISTINCT b.MUSTERI_NO)                                            AS toplam_musteri,
    COUNT(DISTINCT CASE WHEN b.KAMPANYALI = 1 THEN b.MUSTERI_NO END)        AS kampanyali_musteri,

    /* --- WELCOME (BONUS) BITISI — BASELINE outflow baskisi --- */
    /* baseline = kampanyali OLMAYAN musteriler (pure_osawelcome evreni) */
    COUNT(DISTINCT CASE WHEN b.KAMPANYALI = 0
                         AND b.BONUS_BIT_TARIHI BETWEEN w.HAFTA_MIN AND w.HAFTA_MAX
                        THEN b.MUSTERI_NO END)                             AS welcome_bitiyor_bu_hafta,
    COUNT(DISTINCT CASE WHEN b.KAMPANYALI = 0
                         AND b.BONUS_BIT_TARIHI BETWEEN w.HAFTA_MIN + 7 AND w.HAFTA_MAX + 7
                        THEN b.MUSTERI_NO END)                             AS welcome_bitiyor_gelecek_hafta,

    /* --- KAMPANYA BITISI — OVERLAY (ekstra musteri) outflow baskisi --- */
    COUNT(DISTINCT CASE WHEN b.KAMPANYALI = 1
                         AND b.KAMPANYA_BIT_TARIHI BETWEEN w.HAFTA_MIN AND w.HAFTA_MAX
                        THEN b.MUSTERI_NO END)                             AS kampanya_bitiyor_bu_hafta,
    COUNT(DISTINCT CASE WHEN b.KAMPANYALI = 1
                         AND b.KAMPANYA_BIT_TARIHI BETWEEN w.HAFTA_MIN + 7 AND w.HAFTA_MAX + 7
                        THEN b.MUSTERI_NO END)                             AS kampanya_bitiyor_gelecek_hafta
FROM CTE_WEEKS w
JOIN base b ON b.RAPOR_TARIHI BETWEEN w.HAFTA_MIN AND w.HAFTA_MAX
GROUP BY w.HAFTA_MIN, w.HAFTA_MAX
ORDER BY w.HAFTA_MIN;


/* ================================================================
   SORGU C — TOPLAM kampanya envanteri (haftaya bolmeden tek bakis)
   Her KAMPANYA_ADI: toplam musteri, ort/min/max rate, ilk-son gorulme,
   kac farkli bitis tarihi, welcome ustu musteri sayisi.
   "Genel olarak hangi kampanyada kac musteri var" sorusunun cevabi.
   ================================================================ */
WITH
osa_welcome AS (
    SELECT t1.start_date, t1.end_date, t1.rate AS OSAWelcome
    FROM OPR.V_FS_SVIN_DEPOSIT_INTEREST t1
    WHERE t1.deleted = 0 AND t1.interest_code = 1 AND t1.currency_code = 0
),
base_kmp AS (
    SELECT DISTINCT
        t.RAPOR_TARIHI, t.MUSTERI_NO, t.KAMPANYA_ADI, t.KAMPANYA_FAIZ_ORANI,
        t.KAMPANYA_BAS_TARIHI, t.KAMPANYA_BIT_TARIHI,
        CASE WHEN t.KAMPANYA_FAIZ_ORANI > w.OSAWelcome + 0.0001 THEN 1 ELSE 0 END AS WELCOME_USTU
    FROM PRSN.TURUNCU_YENI t
    LEFT JOIN osa_welcome w ON t.RAPOR_TARIHI BETWEEN w.start_date AND w.end_date
    WHERE t.PARA_KODU = 0 AND t.CALISMA_SEKLI = 1
      AND t.RAPOR_TARIHI >= DATE '2024-09-16'
      AND t.KAMPANYA_ADI IS NOT NULL
)
SELECT
    KAMPANYA_ADI,
    COUNT(DISTINCT MUSTERI_NO)                                          AS toplam_musteri,
    ROUND(AVG(KAMPANYA_FAIZ_ORANI), 2)                                 AS ort_rate,
    MIN(KAMPANYA_FAIZ_ORANI)                                           AS min_rate,
    MAX(KAMPANYA_FAIZ_ORANI)                                           AS max_rate,
    COUNT(DISTINCT CASE WHEN WELCOME_USTU = 1 THEN MUSTERI_NO END)     AS welcome_ustu_musteri,
    MIN(RAPOR_TARIHI)                                                  AS ilk_gorulme,
    MAX(RAPOR_TARIHI)                                                  AS son_gorulme,
    MIN(KAMPANYA_BAS_TARIHI)                                           AS en_erken_bas,
    MAX(KAMPANYA_BIT_TARIHI)                                           AS en_gec_bit,
    COUNT(DISTINCT KAMPANYA_BIT_TARIHI)                                AS farkli_bitis_sayisi
FROM base_kmp
GROUP BY KAMPANYA_ADI
ORDER BY toplam_musteri DESC;
