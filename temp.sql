/* ================================================================
   NETFLOW — SADECE STANDART OSAWELCOME MUSTERILERI (baseline hedefi)

   query_netflow ile BIREBIR ayni mantik; TEK EK:
   - Her musteri-gun kaydina o tarihte gecerli STANDART osawelcome baglanir
     (osa_welcome CTE: OPR.V_FS_SVIN_DEPOSIT_INTEREST, interest_code=1, para=0).
   - Musterinin kendi KAMPANYA_FAIZ_ORANI'si standart osawelcome'dan FAZLA ise
     (= ona ekstra/kampanya rate verilmis) o musteri ilgili haftanin akisindan
     TAMAMEN cikarilir.

   NEDEN "tamamen": netflow iki tarih farki (T-1 vs T-2). Musteriyi sadece bir
   ucta cikarirsan sahte OUTFLOW/INFLOW uretir. Bu yuzden musteri iki ucun
   (D_EKSI1 veya D_EKSI2) BIRINDE bile fazla-rate aldiysa, o (hafta,donem)
   akisindan butunuyle dusulur.

   >>> DOGRULANACAK 2 SEY:
   1) KAMPANYA_FAIZ_ORANI: TURUNCU_YENI'deki gercek kolon adiyla degistir.
      (kullanici: "sadece kampanya_faiz_orani var; NaN/osawelcome ile ayni/farkli olabilir")
   2) OLCEK: KAMPANYA_FAIZ_ORANI ile osa_welcome.rate AYNI bazda olmali
      (ham basit oran, orn. 40 / 43). Farkli birimde ise once normalize et.

   NaN/NULL kampanya orani => standart kabul edilir, KORUNUR.
   Kampanya = osawelcome  => standart, KORUNUR.
   Kampanya > osawelcome  => EKSTRA, CIKARILIR.
   (Kampanya < osawelcome => "fazla" degil; KORUNUR.)

   --- OVERLAY (ustu-welcome musterilerinin netflow'u) icin: ---
   CTE_FLOW icindeki  "WHERE NOT EXISTS (... CTE_EXTRA ...)"  satirini
   "WHERE EXISTS (... CTE_EXTRA ...)" yap => sadece ekstra musteriler kalir
   = overlay serisi (gozlenen). Baseline + overlay = toplam netflow.
================================================================ */
WITH

/* -- Tum haftalar (16.09.2024 Pazartesi'den bugune) -- */
CTE_WEEKS AS (
    SELECT
        DATE '2024-09-16' + (LEVEL - 1) * 7       AS HAFTA_MIN,
        DATE '2024-09-16' + (LEVEL - 1) * 7 + 4   AS HAFTA_MAX
    FROM DUAL
    CONNECT BY LEVEL - 1 <= FLOOR((TRUNC(SYSDATE) - DATE '2024-09-16') / 7)
),

/* -- STANDART OSA Hosgeldin orani (tarih araligi bazli) -- */
osa_welcome AS (
    SELECT
        t1.start_date,
        t1.end_date,
        t1.rate AS OSAWelcome
    FROM OPR.V_FS_SVIN_DEPOSIT_INTEREST t1
    WHERE t1.deleted       = 0
      AND t1.interest_code = 1
      AND t1.currency_code = 0
),

/* -- Adim 0: TURUNCU_YENI tek seferde (ilk haftanin onceki Cuma'sindan bugune)
   << KAMPANYA_FAIZ_ORANI da cekiliyor (kolon adini DOGRULA) >> -- */
CTE_BASE AS (
    SELECT /*+ MATERIALIZE PARALLEL(t, 4) */
        t.RAPOR_TARIHI,
        t.MUSTERI_NO,
        t.PARA_KODU,
        t.VADELI_BAKIYE,
        t.VADESIZ_BAKIYE,
        t.KAMPANYA_FAIZ_ORANI   AS KAMPANYA_FAIZ_ORANI   -- << GERCEK KOLON ADIYLA DEGISTIR
    FROM PRSN.TURUNCU_YENI t
    WHERE t.RAPOR_TARIHI BETWEEN (DATE '2024-09-16' - 3) AND TRUNC(SYSDATE)
      AND t.CALISMA_SEKLI = 1
),

/* -- Adim 1: TL bakiyeleri (musteri evreni HAVING ile)
   << CAMP_RATE = musteri-gun kampanya orani da tasiniyor >> -- */
CTE_TL_BAKIYE AS (
    SELECT /*+ MATERIALIZE */
        RAPOR_TARIHI,
        MUSTERI_NO,
        SUM(VADELI_BAKIYE + VADESIZ_BAKIYE) AS TL_BAKIYE,
        MAX(KAMPANYA_FAIZ_ORANI)            AS CAMP_RATE
    FROM CTE_BASE
    WHERE PARA_KODU = 0
    GROUP BY RAPOR_TARIHI, MUSTERI_NO
    HAVING SUM(VADELI_BAKIYE) > 0
),

/* -- EKSTRA musteri-gun kayitlari: kampanya orani standart osawelcome'dan FAZLA
   (o tarihte gecerli standart welcome ile kiyas). Bunlar netflow'dan cikarilacak. -- */
CTE_EXTRA AS (
    SELECT DISTINCT
        b.RAPOR_TARIHI,
        b.MUSTERI_NO
    FROM CTE_TL_BAKIYE b
    JOIN osa_welcome w
      ON b.RAPOR_TARIHI BETWEEN w.start_date AND w.end_date
    WHERE b.CAMP_RATE IS NOT NULL
      AND b.CAMP_RATE > w.OSAWelcome + 0.0001   -- float gurultusu icin kucuk tolerans
),

/* -- Adim 2: Tarih ciftleri
   GUNLUK: Pzt(0)-prev Cuma(-3), Sal(1)-Pzt(0), Car(2)-Sal(1), Per(3)-Car(2), Cum(4)-Per(3)
   HAFTALIK: T_MAX (Cum) vs T_MIN-3 (onceki Cum) -- */
CTE_DATE_PAIRS AS (
    SELECT
        w.HAFTA_MIN,
        w.HAFTA_MAX,
        w.HAFTA_MIN + d.D_BUGUN   AS D_EKSI1,
        w.HAFTA_MIN + d.D_ONCEKI  AS D_EKSI2,
        'GUNLUK'                  AS DONEM_TIPI,
        TO_CHAR(w.HAFTA_MIN + d.D_BUGUN, 'DD.MM.YYYY') AS DONEM_LABEL
    FROM CTE_WEEKS w
    CROSS JOIN (
        SELECT 0 AS D_BUGUN, -3 AS D_ONCEKI FROM DUAL
        UNION ALL SELECT 1,  0 FROM DUAL
        UNION ALL SELECT 2,  1 FROM DUAL
        UNION ALL SELECT 3,  2 FROM DUAL
        UNION ALL SELECT 4,  3 FROM DUAL
    ) d
    UNION ALL
    SELECT
        w.HAFTA_MIN,
        w.HAFTA_MAX,
        w.HAFTA_MAX     AS D_EKSI1,
        w.HAFTA_MIN - 3 AS D_EKSI2,
        'HAFTALIK'      AS DONEM_TIPI,
        TO_CHAR(w.HAFTA_MIN, 'DD.MM.YYYY') || ' - ' || TO_CHAR(w.HAFTA_MAX, 'DD.MM.YYYY') AS DONEM_LABEL
    FROM CTE_WEEKS w
),

/* -- Adim 3: Her tarih cifti icin sadece aktif musteriler -- */
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

/* -- Adim 4: Flow + bracketler (query_netflow ile birebir)
   << EKSTRA musteriler iki ucun BIRINDE bile fazla-rate aldiysa CIKARILIR >> -- */
CTE_FLOW AS (
    SELECT /*+ MATERIALIZE */
        rp.HAFTA_MIN, rp.HAFTA_MAX,
        rp.DONEM_TIPI, rp.DONEM_LABEL,
        rp.MUSTERI_NO,
        NVL(t1.TL_BAKIYE, 0) AS T_EKSI1_BAKIYE,
        NVL(t2.TL_BAKIYE, 0) AS T_EKSI2_BAKIYE,
        CASE
            WHEN NVL(t1.TL_BAKIYE, 0) <= 0 THEN '00:0K'
            WHEN t1.TL_BAKIYE < 10000    THEN '01:0K-10K'
            WHEN t1.TL_BAKIYE < 25000    THEN '02:10K-25K'
            WHEN t1.TL_BAKIYE < 50000    THEN '03:25K-50K'
            WHEN t1.TL_BAKIYE < 75000    THEN '04:50K-75K'
            WHEN t1.TL_BAKIYE < 100000   THEN '05:75K-100K'
            WHEN t1.TL_BAKIYE < 150000   THEN '06:100K-150K'
            WHEN t1.TL_BAKIYE < 250000   THEN '07:150K-250K'
            WHEN t1.TL_BAKIYE < 500000   THEN '08:250K-500K'
            WHEN t1.TL_BAKIYE < 750000   THEN '09:500K-750K'
            WHEN t1.TL_BAKIYE < 1000000  THEN '10:750K-1M'
            WHEN t1.TL_BAKIYE < 2000000  THEN '11:1M-2M'
            WHEN t1.TL_BAKIYE < 5000000  THEN '12:2M-5M'
            WHEN t1.TL_BAKIYE < 7500000  THEN '13:5M-7.5M'
            WHEN t1.TL_BAKIYE < 10000000 THEN '14:7.5M-10M'
            WHEN t1.TL_BAKIYE < 15000000 THEN '15:10M-15M'
            WHEN t1.TL_BAKIYE < 25000000 THEN '16:15M-25M'
            WHEN t1.TL_BAKIYE < 35000000 THEN '17:25M-35M'
            ELSE '18:35M+'
        END AS T_EKSI1_BRACKET,
        CASE
            WHEN NVL(t2.TL_BAKIYE, 0) <= 0 THEN '00:0K'
            WHEN t2.TL_BAKIYE < 10000    THEN '01:0K-10K'
            WHEN t2.TL_BAKIYE < 25000    THEN '02:10K-25K'
            WHEN t2.TL_BAKIYE < 50000    THEN '03:25K-50K'
            WHEN t2.TL_BAKIYE < 75000    THEN '04:50K-75K'
            WHEN t2.TL_BAKIYE < 100000   THEN '05:75K-100K'
            WHEN t2.TL_BAKIYE < 150000   THEN '06:100K-150K'
            WHEN t2.TL_BAKIYE < 250000   THEN '07:150K-250K'
            WHEN t2.TL_BAKIYE < 500000   THEN '08:250K-500K'
            WHEN t2.TL_BAKIYE < 750000   THEN '09:500K-750K'
            WHEN t2.TL_BAKIYE < 1000000  THEN '10:750K-1M'
            WHEN t2.TL_BAKIYE < 2000000  THEN '11:1M-2M'
            WHEN t2.TL_BAKIYE < 5000000  THEN '12:2M-5M'
            WHEN t2.TL_BAKIYE < 7500000  THEN '13:5M-7.5M'
            WHEN t2.TL_BAKIYE < 10000000 THEN '14:7.5M-10M'
            WHEN t2.TL_BAKIYE < 15000000 THEN '15:10M-15M'
            WHEN t2.TL_BAKIYE < 25000000 THEN '16:15M-25M'
            WHEN t2.TL_BAKIYE < 35000000 THEN '17:25M-35M'
            ELSE '18:35M+'
        END AS T_EKSI2_BRACKET,
        NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) AS FARK,
        CASE
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 0        THEN '000: OUTFLOW'
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) = 0        THEN '00:0K'
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 10000    THEN '01:0K-10K'
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 25000    THEN '02:10K-25K'
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 50000    THEN '03:25K-50K'
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 75000    THEN '04:50K-75K'
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 100000   THEN '05:75K-100K'
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 150000   THEN '06:100K-150K'
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 250000   THEN '07:150K-250K'
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 500000   THEN '08:250K-500K'
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 750000   THEN '09:500K-750K'
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 1000000  THEN '10:750K-1M'
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 2000000  THEN '11:1M-2M'
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 5000000  THEN '12:2M-5M'
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 7500000  THEN '13:5M-7.5M'
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 10000000 THEN '14:7.5M-10M'
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 15000000 THEN '15:10M-15M'
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 25000000 THEN '16:15M-25M'
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 35000000 THEN '17:25M-35M'
            ELSE '18:35M+'
        END AS FARK_BRACKET,
        CASE
            WHEN NVL(t1.TL_BAKIYE, 0) - NVL(t2.TL_BAKIYE, 0) < 0 THEN 'OUTFLOW'
            ELSE 'INFLOW'
        END AS INFLOW_OUTFLOW
    FROM CTE_RELEVANT_PAIRS rp
    LEFT JOIN CTE_TL_BAKIYE t1 ON rp.MUSTERI_NO = t1.MUSTERI_NO AND t1.RAPOR_TARIHI = rp.D_EKSI1
    LEFT JOIN CTE_TL_BAKIYE t2 ON rp.MUSTERI_NO = t2.MUSTERI_NO AND t2.RAPOR_TARIHI = rp.D_EKSI2
    /* <<< EKSTRA (ustu-welcome) musterileri AKISTAN CIKAR:
           iki ucun (D_EKSI1 veya D_EKSI2) birinde bile fazla-rate aldiysa dusulur.
           >>> OVERLAY istiyorsan: NOT EXISTS -> EXISTS yap. <<< */
    WHERE NOT EXISTS (
        SELECT 1
        FROM CTE_EXTRA e
        WHERE e.MUSTERI_NO   = rp.MUSTERI_NO
          AND e.RAPOR_TARIHI IN (rp.D_EKSI1, rp.D_EKSI2)
    )
),

/* -- Adim 5: Bracket toplamlari (hafta + donem + bracket bazinda) -- */
CTE_AGG AS (
    SELECT
        COALESCE(A.HAFTA_MIN,   B.HAFTA_MIN,   C.HAFTA_MIN)   AS HAFTA_MIN,
        COALESCE(A.HAFTA_MAX,   B.HAFTA_MAX,   C.HAFTA_MAX)   AS HAFTA_MAX,
        COALESCE(A.DONEM_TIPI,  B.DONEM_TIPI,  C.DONEM_TIPI)  AS DONEM_TIPI,
        COALESCE(A.DONEM_LABEL, B.DONEM_LABEL, C.DONEM_LABEL) AS DONEM_LABEL,
        COALESCE(A.BRACKET,     B.BRACKET,     C.BRACKET)     AS BRACKET,
        NVL(A.INFLOW, 0)      AS INFLOW,
        NVL(C.OUTFLOW, 0)     AS OUTFLOW,
        NVL(B.TOTAL_HACIM, 0) AS TOTAL_HACIM
    FROM (
        SELECT HAFTA_MIN, HAFTA_MAX, DONEM_TIPI, DONEM_LABEL,
               FARK_BRACKET AS BRACKET,
               SUM(FARK)    AS INFLOW
        FROM CTE_FLOW
        WHERE INFLOW_OUTFLOW = 'INFLOW' AND FARK_BRACKET <> '00:0K'
        GROUP BY HAFTA_MIN, HAFTA_MAX, DONEM_TIPI, DONEM_LABEL, FARK_BRACKET
    ) A
    FULL OUTER JOIN (
        SELECT HAFTA_MIN, HAFTA_MAX, DONEM_TIPI, DONEM_LABEL,
               T_EKSI1_BRACKET    AS BRACKET,
               SUM(T_EKSI1_BAKIYE) AS TOTAL_HACIM
        FROM CTE_FLOW
        WHERE T_EKSI1_BRACKET <> '00:0K'
        GROUP BY HAFTA_MIN, HAFTA_MAX, DONEM_TIPI, DONEM_LABEL, T_EKSI1_BRACKET
    ) B
        ON  A.HAFTA_MIN   = B.HAFTA_MIN
        AND A.DONEM_TIPI  = B.DONEM_TIPI
        AND A.DONEM_LABEL = B.DONEM_LABEL
        AND A.BRACKET     = B.BRACKET
    FULL OUTER JOIN (
        SELECT HAFTA_MIN, HAFTA_MAX, DONEM_TIPI, DONEM_LABEL,
               T_EKSI2_BRACKET AS BRACKET,
               SUM(FARK)       AS OUTFLOW
        FROM CTE_FLOW
        WHERE INFLOW_OUTFLOW = 'OUTFLOW' AND T_EKSI2_BRACKET <> '00:0K'
        GROUP BY HAFTA_MIN, HAFTA_MAX, DONEM_TIPI, DONEM_LABEL, T_EKSI2_BRACKET
    ) C
        ON  COALESCE(A.HAFTA_MIN,   B.HAFTA_MIN)   = C.HAFTA_MIN
        AND COALESCE(A.DONEM_TIPI,  B.DONEM_TIPI)  = C.DONEM_TIPI
        AND COALESCE(A.DONEM_LABEL, B.DONEM_LABEL) = C.DONEM_LABEL
        AND COALESCE(A.BRACKET,     B.BRACKET)     = C.BRACKET
    WHERE COALESCE(A.BRACKET, B.BRACKET, C.BRACKET) <> '00:0K'
),

/* -- Adim 6: Haftalik = o haftanin gunluk rate'lerinin BASIT ORTALAMASI -- */
CTE_WEEKLY_AVG AS (
    SELECT
        HAFTA_MIN,
        AVG(ABS(SUM_INFLOW  / NULLIF(SUM_HACIM, 0) * 100)) AS AVG_INFLOW_RATE,
        AVG(ABS(SUM_OUTFLOW / NULLIF(SUM_HACIM, 0) * 100)) AS AVG_OUTFLOW_RATE
    FROM (
        SELECT HAFTA_MIN, DONEM_LABEL,
               SUM(INFLOW)      AS SUM_INFLOW,
               SUM(OUTFLOW)     AS SUM_OUTFLOW,
               SUM(TOTAL_HACIM) AS SUM_HACIM
        FROM CTE_AGG
        WHERE DONEM_TIPI = 'GUNLUK'
        GROUP BY HAFTA_MIN, DONEM_LABEL
    )
    GROUP BY HAFTA_MIN
)

/* -- SON SORGU: her hafta x donem x bracket satiri + rate kolonlari -- */
SELECT
    a.HAFTA_MIN,
    a.HAFTA_MAX,
    a.DONEM_TIPI,
    a.DONEM_LABEL,
    a.BRACKET,
    a.INFLOW,
    a.OUTFLOW,
    a.TOTAL_HACIM,
    CASE
        WHEN a.DONEM_TIPI = 'GUNLUK' THEN
            ABS(ROUND(
                SUM(a.INFLOW)      OVER (PARTITION BY a.HAFTA_MIN, a.DONEM_TIPI, a.DONEM_LABEL) /
                NULLIF(SUM(a.TOTAL_HACIM) OVER (PARTITION BY a.HAFTA_MIN, a.DONEM_TIPI, a.DONEM_LABEL), 0)
                * 100, 6))
        ELSE ROUND(w.AVG_INFLOW_RATE, 6)
    END AS TOTAL_INFLOW_RATE,
    CASE
        WHEN a.DONEM_TIPI = 'GUNLUK' THEN
            ABS(ROUND(
                SUM(a.OUTFLOW)     OVER (PARTITION BY a.HAFTA_MIN, a.DONEM_TIPI, a.DONEM_LABEL) /
                NULLIF(SUM(a.TOTAL_HACIM) OVER (PARTITION BY a.HAFTA_MIN, a.DONEM_TIPI, a.DONEM_LABEL), 0)
                * 100, 6))
        ELSE ROUND(w.AVG_OUTFLOW_RATE, 6)
    END AS TOTAL_OUTFLOW_RATE,
    CASE
        WHEN a.DONEM_TIPI = 'GUNLUK' THEN
            (
                ABS(ROUND(
                    SUM(a.INFLOW) OVER (PARTITION BY a.HAFTA_MIN, a.DONEM_TIPI, a.DONEM_LABEL) /
                    NULLIF(SUM(a.TOTAL_HACIM) OVER (PARTITION BY a.HAFTA_MIN, a.DONEM_TIPI, a.DONEM_LABEL), 0)
                    * 100, 6))
                -
                ABS(ROUND(
                    SUM(a.OUTFLOW) OVER (PARTITION BY a.HAFTA_MIN, a.DONEM_TIPI, a.DONEM_LABEL) /
                    NULLIF(SUM(a.TOTAL_HACIM) OVER (PARTITION BY a.HAFTA_MIN, a.DONEM_TIPI, a.DONEM_LABEL), 0)
                    * 100, 6))
            )
        ELSE ROUND(w.AVG_INFLOW_RATE - w.AVG_OUTFLOW_RATE, 6)
    END AS NETFLOW_RATE
FROM CTE_AGG a
JOIN CTE_WEEKLY_AVG w ON a.HAFTA_MIN = w.HAFTA_MIN
ORDER BY a.HAFTA_MIN, a.DONEM_TIPI DESC, a.DONEM_LABEL, a.BRACKET
