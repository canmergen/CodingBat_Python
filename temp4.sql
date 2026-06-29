/* ================================================================
   KAMPANYA / WELCOME HAFTALIK TABLO — TURUNCU_YENI uzerinden.
   Haftada TEK satir: o hafta ne olmus.

   Kavramlar (kullanici teyidi):
   - BONUS_*    = STANDART OSAWelcome (HERKESTE). BONUS_BIT_TARIHI = welcome bitisi.
   - KAMPANYA_* = bazi musterilere EKSTRA kampanya. KAMPANYA_BIT_TARIHI = ekstranin bitisi.
   - "ekstra musteri" = KAMPANYA_ADI IS NOT NULL.

   Kolonlar:
   - toplam_musteri / kampanyali_musteri / baseline_musteri
   - farkli_kampanya_sayisi              : o hafta kac AYRI kampanya adi aktif
   - en_buyuk_kampanya (+ _musteri)      : o haftanin en cok musterili kampanyasi
   - welcome_ustu_musteri                : KAMPANYA_FAIZ_ORANI > welcome olan
   - welcome_bitiyor_bu/gelecek_hafta    : BASELINE musterilerin welcome bitisi (outflow baskisi)
   - kampanya_bitiyor_bu/gelecek_hafta   : EKSTRA musterilerin kampanya bitisi (overlay)

   Hafta: Pzt(16.09.2024)-Cuma. Kolon adlarini gerekirse degistir.
   ================================================================ */
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
/* tum aktif TL musteri-gun + kampanya/bonus alanlari + bayraklar */
base AS (
    SELECT DISTINCT
        t.RAPOR_TARIHI,
        t.MUSTERI_NO,
        t.KAMPANYA_ADI,
        t.BONUS_BIT_TARIHI,
        t.KAMPANYA_BIT_TARIHI,
        CASE WHEN t.KAMPANYA_ADI IS NOT NULL THEN 1 ELSE 0 END                AS KAMPANYALI,
        CASE WHEN t.KAMPANYA_FAIZ_ORANI > w.OSAWelcome + 0.0001 THEN 1 ELSE 0 END AS WELCOME_USTU
    FROM PRSN.TURUNCU_YENI t
    LEFT JOIN osa_welcome w ON t.RAPOR_TARIHI BETWEEN w.start_date AND w.end_date
    WHERE t.PARA_KODU = 0 AND t.CALISMA_SEKLI = 1
      AND t.RAPOR_TARIHI >= DATE '2024-09-16'
),
/* hafta-level toplamlar (tum musteriler) */
wk_all AS (
    SELECT
        w.HAFTA_MIN, w.HAFTA_MAX,
        COUNT(DISTINCT b.MUSTERI_NO)                                          AS toplam_musteri,
        COUNT(DISTINCT CASE WHEN b.KAMPANYALI = 1 THEN b.MUSTERI_NO END)      AS kampanyali_musteri,
        COUNT(DISTINCT CASE WHEN b.KAMPANYALI = 0 THEN b.MUSTERI_NO END)      AS baseline_musteri,
        COUNT(DISTINCT b.KAMPANYA_ADI)                                        AS farkli_kampanya_sayisi,
        COUNT(DISTINCT CASE WHEN b.WELCOME_USTU = 1 THEN b.MUSTERI_NO END)    AS welcome_ustu_musteri,
        COUNT(DISTINCT CASE WHEN b.KAMPANYALI = 0
                             AND b.BONUS_BIT_TARIHI BETWEEN w.HAFTA_MIN AND w.HAFTA_MAX
                            THEN b.MUSTERI_NO END)                            AS welcome_bitiyor_bu_hafta,
        COUNT(DISTINCT CASE WHEN b.KAMPANYALI = 0
                             AND b.BONUS_BIT_TARIHI BETWEEN w.HAFTA_MIN + 7 AND w.HAFTA_MAX + 7
                            THEN b.MUSTERI_NO END)                            AS welcome_bitiyor_gelecek_hafta,
        COUNT(DISTINCT CASE WHEN b.KAMPANYALI = 1
                             AND b.KAMPANYA_BIT_TARIHI BETWEEN w.HAFTA_MIN AND w.HAFTA_MAX
                            THEN b.MUSTERI_NO END)                            AS kampanya_bitiyor_bu_hafta,
        COUNT(DISTINCT CASE WHEN b.KAMPANYALI = 1
                             AND b.KAMPANYA_BIT_TARIHI BETWEEN w.HAFTA_MIN + 7 AND w.HAFTA_MAX + 7
                            THEN b.MUSTERI_NO END)                            AS kampanya_bitiyor_gelecek_hafta
    FROM CTE_WEEKS w
    JOIN base b ON b.RAPOR_TARIHI BETWEEN w.HAFTA_MIN AND w.HAFTA_MAX
    GROUP BY w.HAFTA_MIN, w.HAFTA_MAX
),
/* hafta x kampanya_adi musteri sayisi (en buyuk kampanyayi bulmak icin) */
wk_kmp AS (
    SELECT w.HAFTA_MIN, b.KAMPANYA_ADI,
           COUNT(DISTINCT b.MUSTERI_NO) AS cnt
    FROM CTE_WEEKS w
    JOIN base b ON b.RAPOR_TARIHI BETWEEN w.HAFTA_MIN AND w.HAFTA_MAX
    WHERE b.KAMPANYALI = 1
    GROUP BY w.HAFTA_MIN, b.KAMPANYA_ADI
),
/* o haftanin en cok musterili kampanyasi */
wk_top AS (
    SELECT HAFTA_MIN,
           MAX(KAMPANYA_ADI) KEEP (DENSE_RANK LAST ORDER BY cnt, KAMPANYA_ADI) AS en_buyuk_kampanya,
           MAX(cnt)          KEEP (DENSE_RANK LAST ORDER BY cnt, KAMPANYA_ADI) AS en_buyuk_kampanya_musteri
    FROM wk_kmp
    GROUP BY HAFTA_MIN
)
SELECT
    a.HAFTA_MIN,
    a.HAFTA_MAX,
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
