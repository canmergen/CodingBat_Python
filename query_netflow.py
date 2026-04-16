query_netflow = '''
/* ================================================================
  Tum Pazartesi-Cuma haftalik periyotlar icin
  inflow/outflow bracket analizi. Tek sorgu - tum haftalar.
  Hafta kurgusu:
    Hafta Basi = Pazartesi  (HAFTA_MIN)
    Hafta Sonu = Cuma       (HAFTA_MAX)
    Ilk Hafta  = 30.09.2024 - 04.10.2024
    Referans   = 30.09.2024 (Pazartesi)
  Cikti: her hafta x her bracket icin 1 satir
    DONEM_TIPI = GUNLUK  : haftanin her is gunu ayri ayri
    DONEM_TIPI = HAFTALIK : (toplam haftalik flow / hafta basi bakiye) / is gunu sayisi
  HAFTALIK rate is gunu sayisina bolunerek normalize edilir.
  IS_GUNU_SAYISI feature olarak ciktida yer alir.
================================================================ */
WITH
/* -- Tum haftalar (30.09.2024 Pazartesiden bugune, Pzt-Cuma) -- */
CTE_WEEKS AS (
SELECT
     DATE '2024-09-30' + (LEVEL - 1) * 7       AS HAFTA_MIN,
     DATE '2024-09-30' + (LEVEL - 1) * 7 + 4   AS HAFTA_MAX
FROM DUAL
CONNECT BY LEVEL - 1 <= FLOOR((TRUNC(SYSDATE) - DATE '2024-09-30') / 7)
),
/* -- Tum donem icin tek seferlik tablo okuma
   Baslangic: 27.09.2024 (ilk Pzt oncesi Cuma, haftalik flow icin) -- */
CTE_BASE AS (
SELECT /*+ MATERIALIZE PARALLEL(t, 4) */
     t.RAPOR_TARIHI,
     t.MUSTERI_NO,
     t.VADELI_BAKIYE,
     t.VADESIZ_BAKIYE
FROM PRSN.TURUNCU_YENI t
WHERE t.PARA_KODU        = 0
   AND t.CALISMA_SEKLI   = 1
   AND t.RAPOR_TARIHI    >= DATE '2024-09-27'
   AND t.RAPOR_TARIHI    <= TRUNC(SYSDATE)
   AND t.TH_TOTAL_BAKIYE  > 0
   AND t.URUN_KODU       <> 417
),
/* -- TL bakiye (musteri x gun) -- */
CTE_TL_BAKIYE AS (
SELECT /*+ MATERIALIZE */
     RAPOR_TARIHI,
     MUSTERI_NO,
     SUM(VADELI_BAKIYE + VADESIZ_BAKIYE) AS TL_BAKIYE
FROM CTE_BASE
GROUP BY RAPOR_TARIHI, MUSTERI_NO
HAVING SUM(VADELI_BAKIYE) > 0
),
/* -- Her hafta x her is gunu icin tarih ciftleri
   Pzt(0) vs onceki Cuma(-3), Sal(1) vs Pzt(0),
   Car(2) vs Sal(1), Per(3) vs Car(2), Cum(4) vs Per(3)
   HAFTALIK: Cuma vs onceki Cuma -- */
CTE_DATE_PAIRS AS (
SELECT
     w.HAFTA_MIN,
     w.HAFTA_MAX,
     w.HAFTA_MIN + d.D_BUGUN    AS D_EKSI1,
     w.HAFTA_MIN + d.D_ONCEKI   AS D_EKSI2,
     'GUNLUK'                    AS DONEM_TIPI,
     TO_CHAR(w.HAFTA_MIN + d.D_BUGUN, 'DD.MM.YYYY') AS DONEM_LABEL
FROM CTE_WEEKS w
CROSS JOIN (
     SELECT  0 AS D_BUGUN, -3 AS D_ONCEKI FROM DUAL
     UNION ALL SELECT  1,  0 FROM DUAL
     UNION ALL SELECT  2,  1 FROM DUAL
     UNION ALL SELECT  3,  2 FROM DUAL
     UNION ALL SELECT  4,  3 FROM DUAL
) d
UNION ALL
SELECT
     w.HAFTA_MIN,
     w.HAFTA_MAX,
     w.HAFTA_MAX,
     w.HAFTA_MIN - 3,
     'HAFTALIK',
     TO_CHAR(w.HAFTA_MIN, 'DD.MM.YYYY') || ' - ' || TO_CHAR(w.HAFTA_MAX, 'DD.MM.YYYY')
FROM CTE_WEEKS w
),
/* -- Her tarih cifti icin aktif musteriler
   OR join yerine UNION kullanilir (performans icin) -- */
CTE_RELEVANT_PAIRS AS (
SELECT
     dp.HAFTA_MIN, dp.HAFTA_MAX,
     dp.DONEM_TIPI, dp.DONEM_LABEL,
     dp.D_EKSI1, dp.D_EKSI2,
     b.MUSTERI_NO
FROM CTE_DATE_PAIRS dp
JOIN CTE_TL_BAKIYE b ON b.RAPOR_TARIHI = dp.D_EKSI1

UNION

SELECT
     dp.HAFTA_MIN, dp.HAFTA_MAX,
     dp.DONEM_TIPI, dp.DONEM_LABEL,
     dp.D_EKSI1, dp.D_EKSI2,
     b.MUSTERI_NO
FROM CTE_DATE_PAIRS dp
JOIN CTE_TL_BAKIYE b ON b.RAPOR_TARIHI = dp.D_EKSI2
),
/* -- Flow hesabi + bracket etiketleme -- */
CTE_FLOW AS (
SELECT /*+ MATERIALIZE */
     rp.HAFTA_MIN,
     rp.HAFTA_MAX,
     rp.DONEM_TIPI,
     rp.DONEM_LABEL,
     rp.MUSTERI_NO,
     NVL(t1.TL_BAKIYE, 0)                           AS T_EKSI1_BAKIYE,
     NVL(t2.TL_BAKIYE, 0)                           AS T_EKSI2_BAKIYE,
     NVL(t1.TL_BAKIYE,0) - NVL(t2.TL_BAKIYE,0)     AS FARK,
     /* T-1 (bugunun) bakiye segmenti */
     CASE
         WHEN NVL(t1.TL_BAKIYE,0) <=        0 THEN '00:0K'
         WHEN t1.TL_BAKIYE          <    10000 THEN '01:0K-10K'
         WHEN t1.TL_BAKIYE          <    25000 THEN '02:10K-25K'
         WHEN t1.TL_BAKIYE          <    50000 THEN '03:25K-50K'
         WHEN t1.TL_BAKIYE          <    75000 THEN '04:50K-75K'
         WHEN t1.TL_BAKIYE          <   100000 THEN '05:75K-100K'
         WHEN t1.TL_BAKIYE          <   150000 THEN '06:100K-150K'
         WHEN t1.TL_BAKIYE          <   250000 THEN '07:150K-250K'
         WHEN t1.TL_BAKIYE          <   500000 THEN '08:250K-500K'
         WHEN t1.TL_BAKIYE          <   750000 THEN '09:500K-750K'
         WHEN t1.TL_BAKIYE          <  1000000 THEN '10:750K-1M'
         WHEN t1.TL_BAKIYE          <  2000000 THEN '11:1M-2M'
         WHEN t1.TL_BAKIYE          <  5000000 THEN '12:2M-5M'
         WHEN t1.TL_BAKIYE          <  7500000 THEN '13:5M-7.5M'
         WHEN t1.TL_BAKIYE          < 10000000 THEN '14:7.5M-10M'
         WHEN t1.TL_BAKIYE          < 15000000 THEN '15:10M-15M'
         WHEN t1.TL_BAKIYE          < 25000000 THEN '16:15M-25M'
         WHEN t1.TL_BAKIYE          < 35000000 THEN '17:25M-35M'
         ELSE                                       '18:35M+'
     END AS T_EKSI1_BRACKET,
     /* T-2 (onceki gunun) bakiye segmenti */
     CASE
         WHEN NVL(t2.TL_BAKIYE,0) <=        0 THEN '00:0K'
         WHEN t2.TL_BAKIYE          <    10000 THEN '01:0K-10K'
         WHEN t2.TL_BAKIYE          <    25000 THEN '02:10K-25K'
         WHEN t2.TL_BAKIYE          <    50000 THEN '03:25K-50K'
         WHEN t2.TL_BAKIYE          <    75000 THEN '04:50K-75K'
         WHEN t2.TL_BAKIYE          <   100000 THEN '05:75K-100K'
         WHEN t2.TL_BAKIYE          <   150000 THEN '06:100K-150K'
         WHEN t2.TL_BAKIYE          <   250000 THEN '07:150K-250K'
         WHEN t2.TL_BAKIYE          <   500000 THEN '08:250K-500K'
         WHEN t2.TL_BAKIYE          <   750000 THEN '09:500K-750K'
         WHEN t2.TL_BAKIYE          <  1000000 THEN '10:750K-1M'
         WHEN t2.TL_BAKIYE          <  2000000 THEN '11:1M-2M'
         WHEN t2.TL_BAKIYE          <  5000000 THEN '12:2M-5M'
         WHEN t2.TL_BAKIYE          <  7500000 THEN '13:5M-7.5M'
         WHEN t2.TL_BAKIYE          < 10000000 THEN '14:7.5M-10M'
         WHEN t2.TL_BAKIYE          < 15000000 THEN '15:10M-15M'
         WHEN t2.TL_BAKIYE          < 25000000 THEN '16:15M-25M'
         WHEN t2.TL_BAKIYE          < 35000000 THEN '17:25M-35M'
         ELSE                                       '18:35M+'
     END AS T_EKSI2_BRACKET,
     /* Fark (flow) segmenti */
     CASE
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) <        0 THEN '000: OUTFLOW'
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) =        0 THEN '00:0K'
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) <    10000 THEN '01:0K-10K'
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) <    25000 THEN '02:10K-25K'
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) <    50000 THEN '03:25K-50K'
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) <    75000 THEN '04:50K-75K'
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) <   100000 THEN '05:75K-100K'
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) <   150000 THEN '06:100K-150K'
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) <   250000 THEN '07:150K-250K'
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) <   500000 THEN '08:250K-500K'
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) <   750000 THEN '09:500K-750K'
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) <  1000000 THEN '10:750K-1M'
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) <  2000000 THEN '11:1M-2M'
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) <  5000000 THEN '12:2M-5M'
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) <  7500000 THEN '13:5M-7.5M'
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) < 10000000 THEN '14:7.5M-10M'
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) < 15000000 THEN '15:10M-15M'
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) < 25000000 THEN '16:15M-25M'
         WHEN NVL(t1.TL_BAKIYE,0)-NVL(t2.TL_BAKIYE,0) < 35000000 THEN '17:25M-35M'
         ELSE                                                            '18:35M+'
     END AS FARK_BRACKET,
     /* Inflow / Outflow yonu */
     CASE
         WHEN NVL(t1.TL_BAKIYE,0) - NVL(t2.TL_BAKIYE,0) < 0
         THEN 'OUTFLOW' ELSE 'INFLOW'
     END AS INFLOW_OUTFLOW
FROM CTE_RELEVANT_PAIRS rp
LEFT JOIN CTE_TL_BAKIYE t1
     ON rp.MUSTERI_NO = t1.MUSTERI_NO AND t1.RAPOR_TARIHI = rp.D_EKSI1
LEFT JOIN CTE_TL_BAKIYE t2
     ON rp.MUSTERI_NO = t2.MUSTERI_NO AND t2.RAPOR_TARIHI = rp.D_EKSI2
),
/* -- Bracket agregasyon (hafta + bracket bazinda)
   Inflow, outflow ve toplam hacim ayri ayri gruplanip
   full outer join ile birlestirilir -- */
CTE_AGG AS (
SELECT
     COALESCE(A.HAFTA_MIN,   B.HAFTA_MIN,   C.HAFTA_MIN)   AS HAFTA_MIN,
     COALESCE(A.HAFTA_MAX,   B.HAFTA_MAX,   C.HAFTA_MAX)   AS HAFTA_MAX,
     COALESCE(A.DONEM_TIPI,  B.DONEM_TIPI,  C.DONEM_TIPI)  AS DONEM_TIPI,
     COALESCE(A.DONEM_LABEL, B.DONEM_LABEL, C.DONEM_LABEL) AS DONEM_LABEL,
     COALESCE(A.BRACKET,     B.BRACKET,     C.BRACKET)     AS BRACKET,
     NVL(A.INFLOW,      0) AS INFLOW,
     NVL(C.OUTFLOW,     0) AS OUTFLOW,
     NVL(B.TOTAL_HACIM, 0) AS TOTAL_HACIM
FROM (
     SELECT HAFTA_MIN, HAFTA_MAX, DONEM_TIPI, DONEM_LABEL,
            FARK_BRACKET AS BRACKET, SUM(FARK) AS INFLOW
     FROM CTE_FLOW
     WHERE INFLOW_OUTFLOW = 'INFLOW' AND FARK_BRACKET <> '00:0K'
     GROUP BY HAFTA_MIN, HAFTA_MAX, DONEM_TIPI, DONEM_LABEL, FARK_BRACKET
) A
FULL OUTER JOIN (
     SELECT HAFTA_MIN, HAFTA_MAX, DONEM_TIPI, DONEM_LABEL,
            T_EKSI1_BRACKET AS BRACKET, SUM(T_EKSI1_BAKIYE) AS TOTAL_HACIM
     FROM CTE_FLOW
     WHERE T_EKSI1_BRACKET <> '00:0K'
     GROUP BY HAFTA_MIN, HAFTA_MAX, DONEM_TIPI, DONEM_LABEL, T_EKSI1_BRACKET
) B ON  A.HAFTA_MIN   = B.HAFTA_MIN
     AND A.DONEM_TIPI  = B.DONEM_TIPI
     AND A.DONEM_LABEL = B.DONEM_LABEL
     AND A.BRACKET     = B.BRACKET
FULL OUTER JOIN (
     SELECT HAFTA_MIN, HAFTA_MAX, DONEM_TIPI, DONEM_LABEL,
            T_EKSI2_BRACKET AS BRACKET, SUM(FARK) AS OUTFLOW
     FROM CTE_FLOW
     WHERE INFLOW_OUTFLOW = 'OUTFLOW' AND T_EKSI2_BRACKET <> '00:0K'
     GROUP BY HAFTA_MIN, HAFTA_MAX, DONEM_TIPI, DONEM_LABEL, T_EKSI2_BRACKET
) C ON  COALESCE(A.HAFTA_MIN,   B.HAFTA_MIN)   = C.HAFTA_MIN
     AND COALESCE(A.DONEM_TIPI,  B.DONEM_TIPI)  = C.DONEM_TIPI
     AND COALESCE(A.DONEM_LABEL, B.DONEM_LABEL) = C.DONEM_LABEL
     AND COALESCE(A.BRACKET,     B.BRACKET)     = C.BRACKET
WHERE COALESCE(A.BRACKET, B.BRACKET, C.BRACKET) <> '00:0K'
),
/* -- Her hafta icin gercek is gunu sayisi -- */
CTE_IS_GUNU AS (
SELECT
     HAFTA_MIN,
     COUNT(DISTINCT DONEM_LABEL) AS IS_GUNU_SAYISI
FROM CTE_FLOW
WHERE DONEM_TIPI = 'GUNLUK'
GROUP BY HAFTA_MIN
),
/* -- Haftalik oran: toplam flow / hafta basi bakiye / is gunu sayisi -- */
CTE_WEEKLY_TOTAL AS (
SELECT
     f.HAFTA_MIN,
     SUM(CASE WHEN f.FARK > 0 THEN f.FARK ELSE 0 END)
         / NULLIF(SUM(f.T_EKSI2_BAKIYE), 0) * 100
         / g.IS_GUNU_SAYISI           AS TOTAL_INFLOW_RATE,
     ABS(SUM(CASE WHEN f.FARK < 0 THEN f.FARK ELSE 0 END))
         / NULLIF(SUM(f.T_EKSI2_BAKIYE), 0) * 100
         / g.IS_GUNU_SAYISI           AS TOTAL_OUTFLOW_RATE,
     g.IS_GUNU_SAYISI
FROM CTE_FLOW f
JOIN CTE_IS_GUNU g ON f.HAFTA_MIN = g.HAFTA_MIN
WHERE f.DONEM_TIPI = 'HAFTALIK'
GROUP BY f.HAFTA_MIN, g.IS_GUNU_SAYISI
)
/* -- Son cikti: hafta x bracket bazinda inflow/outflow ve rate -- */
SELECT
a.HAFTA_MIN, a.HAFTA_MAX, a.DONEM_TIPI, a.DONEM_LABEL, a.BRACKET,
a.INFLOW, a.OUTFLOW, a.TOTAL_HACIM,
wt.IS_GUNU_SAYISI,
CASE
     WHEN a.DONEM_TIPI = 'GUNLUK' THEN
         ABS(ROUND(
             SUM(a.INFLOW)  OVER (PARTITION BY a.HAFTA_MIN, a.DONEM_LABEL)
             / NULLIF(SUM(a.TOTAL_HACIM) OVER (PARTITION BY a.HAFTA_MIN, a.DONEM_LABEL), 0)
             * 100, 6))
     ELSE ROUND(wt.TOTAL_INFLOW_RATE, 6)
END AS TOTAL_INFLOW_RATE,
CASE
     WHEN a.DONEM_TIPI = 'GUNLUK' THEN
         ABS(ROUND(
             SUM(a.OUTFLOW) OVER (PARTITION BY a.HAFTA_MIN, a.DONEM_LABEL)
             / NULLIF(SUM(a.TOTAL_HACIM) OVER (PARTITION BY a.HAFTA_MIN, a.DONEM_LABEL), 0)
             * 100, 6))
     ELSE ROUND(wt.TOTAL_OUTFLOW_RATE, 6)
END AS TOTAL_OUTFLOW_RATE,
CASE
     WHEN a.DONEM_TIPI = 'GUNLUK' THEN
         ABS(ROUND(SUM(a.INFLOW)  OVER (PARTITION BY a.HAFTA_MIN, a.DONEM_LABEL)
             / NULLIF(SUM(a.TOTAL_HACIM) OVER (PARTITION BY a.HAFTA_MIN, a.DONEM_LABEL), 0) * 100, 6))
         -
         ABS(ROUND(SUM(a.OUTFLOW) OVER (PARTITION BY a.HAFTA_MIN, a.DONEM_LABEL)
             / NULLIF(SUM(a.TOTAL_HACIM) OVER (PARTITION BY a.HAFTA_MIN, a.DONEM_LABEL), 0) * 100, 6))
     ELSE ROUND(wt.TOTAL_INFLOW_RATE - wt.TOTAL_OUTFLOW_RATE, 6)
END AS NETFLOW_RATE
FROM CTE_AGG a
JOIN CTE_WEEKLY_TOTAL wt ON a.HAFTA_MIN = wt.HAFTA_MIN
ORDER BY a.HAFTA_MIN, a.DONEM_TIPI DESC, a.DONEM_LABEL, a.BRACKET
'''
