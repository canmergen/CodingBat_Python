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
        BONUS_BIT_TARIHI, KAMPANYA_BAS_TARIHI, KAMPANYA_BIT_TARIHI, KAMPANYA_FAIZ_ORANI, TH_TOTAL_BAKIYE
    FROM (
        SELECT /*+ PARALLEL(t, 4) */
            t.MUSTERI_NO,
            t.KAMPANYA_ADI,
            t.BONUS_BIT_TARIHI,
            t.KAMPANYA_BAS_TARIHI,
            t.KAMPANYA_BIT_TARIHI,
            t.KAMPANYA_FAIZ_ORANI,
            t.TH_TOTAL_BAKIYE,
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
        MAX(KAMPANYA_BAS_TARIHI)                                  AS KAMPANYA_BAS_TARIHI,
        MAX(KAMPANYA_BIT_TARIHI)                                  AS KAMPANYA_BIT_TARIHI,
        MAX(KAMPANYA_ADI)                                         AS KAMPANYA_ADI,
        SUM(TH_TOTAL_BAKIYE)                                      AS MUSTERI_HACIM
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
        c.BONUS_BIT_TARIHI, c.KAMPANYA_BAS_TARIHI, c.KAMPANYA_BIT_TARIHI, c.MUSTERI_HACIM,
        c.KAMPANYA_FAIZ_ORANI, v.OSAWelcome,
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
        SUM(MUSTERI_HACIM)                            AS toplam_hacim,
        SUM(CASE WHEN KAMPANYALI = 1 THEN MUSTERI_HACIM ELSE 0 END) AS kampanyali_hacim,
        SUM(CASE WHEN KAMPANYALI = 0 THEN MUSTERI_HACIM ELSE 0 END) AS baseline_hacim,
        SUM(CASE WHEN WELCOME_USTU = 1 THEN MUSTERI_HACIM ELSE 0 END) AS welcome_ustu_hacim,
        SUM(CASE WHEN KAMPANYALI = 0
                  AND BONUS_BIT_TARIHI >= HAFTA_MIN     AND BONUS_BIT_TARIHI < HAFTA_MIN + 7
                 THEN 1 ELSE 0 END)    AS welcome_bitiyor_bu_hafta,
        SUM(CASE WHEN KAMPANYALI = 0
                  AND BONUS_BIT_TARIHI >= HAFTA_MIN + 7 AND BONUS_BIT_TARIHI < HAFTA_MIN + 14
                 THEN 1 ELSE 0 END)    AS welcome_bitiyor_gelecek_hafta,
        SUM(CASE WHEN KAMPANYALI = 1
                  AND KAMPANYA_BIT_TARIHI >= HAFTA_MIN + 7 AND KAMPANYA_BIT_TARIHI < HAFTA_MIN + 14
                 THEN 1 ELSE 0 END)    AS kampanya_bitiyor_gelecek_hafta,
        /* bitis HACIMLERI (TL bazinda bitis baskisi) */
        SUM(CASE WHEN KAMPANYALI = 0 AND BONUS_BIT_TARIHI >= HAFTA_MIN     AND BONUS_BIT_TARIHI < HAFTA_MIN + 7
                 THEN MUSTERI_HACIM ELSE 0 END) AS welcome_bitiyor_bu_hafta_hacim,
        SUM(CASE WHEN KAMPANYALI = 0 AND BONUS_BIT_TARIHI >= HAFTA_MIN + 7 AND BONUS_BIT_TARIHI < HAFTA_MIN + 14
                 THEN MUSTERI_HACIM ELSE 0 END) AS welcome_bitiyor_gelecek_hafta_hacim,
        SUM(CASE WHEN KAMPANYALI = 1 AND KAMPANYA_BIT_TARIHI >= HAFTA_MIN + 7 AND KAMPANYA_BIT_TARIHI < HAFTA_MIN + 14
                 THEN MUSTERI_HACIM ELSE 0 END) AS kampanya_bitiyor_gelecek_hafta_hacim,
        /* kampanya BASLIYOR (bu hafta basladi; snapshot'ta aktif oldugu icin kayipsiz) */
        SUM(CASE WHEN KAMPANYALI = 1 AND KAMPANYA_BAS_TARIHI >= HAFTA_MIN AND KAMPANYA_BAS_TARIHI < HAFTA_MIN + 7
                 THEN 1 ELSE 0 END)            AS kampanya_basliyor_bu_hafta,
        SUM(CASE WHEN KAMPANYALI = 1 AND KAMPANYA_BAS_TARIHI >= HAFTA_MIN AND KAMPANYA_BAS_TARIHI < HAFTA_MIN + 7
                 THEN MUSTERI_HACIM ELSE 0 END) AS kampanya_basliyor_bu_hafta_hacim,
        /* kampanya rate ortalamasi + welcome primi (sadece kampanyalilarda) */
        ROUND(AVG(CASE WHEN KAMPANYALI = 1 THEN KAMPANYA_FAIZ_ORANI END), 2)             AS ort_kampanya_rate,
        ROUND(AVG(CASE WHEN KAMPANYALI = 1 THEN KAMPANYA_FAIZ_ORANI - OSAWelcome END), 2) AS kampanya_primi
    FROM cw
    GROUP BY HAFTA_MIN
)
/* ----------------------------------------------------------------
   KOLON SOZLUGU (her feature ne anlatir):
   MIN/MAX_HAFTA            : haftanin Pzt / Cuma'si
   -- SAYI (adet) --
   toplam_musteri           : o hafta aktif TL musteri (son is gunu snapshot)
   kampanyali_musteri       : ekstra kampanyasi olan (KAMPANYA_ADI dolu)
   baseline_musteri         : kampanyasiz (pure_osawelcome evreni)
   welcome_ustu_musteri     : KAMPANYA_FAIZ_ORANI > o gunku welcome olan
   farkli_kampanya_sayisi   : o hafta kac AYRI kampanya adi aktif
   -- SAYI ORANI (adet payi, 0-1) --
   kampanyali_orani         : kampanyali / toplam        (kitlenin ne kadari kampanyali)
   baseline_orani           : baseline / toplam
   welcome_ustu_orani       : welcome_ustu / toplam
   -- HACIM (TH_TOTAL_BAKIYE, TL) --
   toplam_hacim             : tum musterilerin bakiye toplami
   kampanyali_hacim         : kampanyalilarin bakiyesi
   baseline_hacim           : baseline bakiyesi
   welcome_ustu_hacim       : welcome ustu musterilerin bakiyesi
   -- HACIM ORANI (bakiye payi, 0-1) --
   kampanyali_hacim_orani   : kampanyali_hacim / toplam_hacim
       >>> kampanyali_orani (adet) DUSUK ama bu YUKSEK ise: az sayida AMA buyuk-bakiyeli musteriye ekstra rate
   baseline_hacim_orani / welcome_ustu_hacim_orani : ayni mantik
   -- ORTALAMA BAKIYE (musteri basi, TL) --
   kampanyali_ort_bakiye    : kampanyali_hacim / kampanyali_musteri (kampanyali tipik buyukluk)
   baseline_ort_bakiye      : baseline tipik buyukluk
       >>> kampanyali_ort_bakiye >> baseline_ort_bakiye ise: kampanya buyuk musteriye gidiyor
   -- KAMPANYA RATE --
   ort_kampanya_rate        : kampanyalilarin ortalama KAMPANYA_FAIZ_ORANI (kampanya agresifligi)
   kampanya_primi           : ort(KAMPANYA_FAIZ_ORANI - welcome) — welcome ustu prim buyuklugu
   -- BITIS (outflow oncusu) --
   welcome_bitiyor_bu/gelecek_hafta        : baseline welcome'i biten musteri ADEDI
   welcome_bitiyor_*_hacim                  : ayni, TL bazinda (asil risk olcusu)
   kampanya_bitiyor_gelecek_hafta(_hacim)   : ekstra kampanyasi biten (adet / TL)
   -- BASLANGIC (inflow oncusu) --
   kampanya_basliyor_bu_hafta(_hacim)       : bu hafta yeni kampanya tanimlanan (adet / TL)
   -- DEGISIM (WoW momentum) --
   *_degisim                : bu hafta - gecen hafta (LAG); ilk hafta NULL
   ---------------------------------------------------------------- */
SELECT
    a.HAFTA_MIN                      AS MIN_HAFTA,
    a.HAFTA_MIN + 4                  AS MAX_HAFTA,
    /* --- SAYI (adet) --- */
    a.toplam_musteri,
    a.kampanyali_musteri,
    a.baseline_musteri,
    a.welcome_ustu_musteri,
    a.farkli_kampanya_sayisi,
    /* --- SAYI ORANI (musteri adedine gore pay) --- */
    ROUND(a.kampanyali_musteri  / NULLIF(a.toplam_musteri, 0), 4) AS kampanyali_orani,
    ROUND(a.baseline_musteri    / NULLIF(a.toplam_musteri, 0), 4) AS baseline_orani,
    ROUND(a.welcome_ustu_musteri/ NULLIF(a.toplam_musteri, 0), 4) AS welcome_ustu_orani,
    /* --- HACIM (TH_TOTAL_BAKIYE) --- */
    a.toplam_hacim,
    a.kampanyali_hacim,
    a.baseline_hacim,
    a.welcome_ustu_hacim,
    /* --- HACIM ORANI (bakiye payi) --- */
    ROUND(a.kampanyali_hacim  / NULLIF(a.toplam_hacim, 0), 4) AS kampanyali_hacim_orani,
    ROUND(a.baseline_hacim    / NULLIF(a.toplam_hacim, 0), 4) AS baseline_hacim_orani,
    ROUND(a.welcome_ustu_hacim/ NULLIF(a.toplam_hacim, 0), 4) AS welcome_ustu_hacim_orani,
    /* --- ORTALAMA BAKIYE (musteri basi) --- */
    ROUND(a.kampanyali_hacim / NULLIF(a.kampanyali_musteri, 0), 0) AS kampanyali_ort_bakiye,
    ROUND(a.baseline_hacim   / NULLIF(a.baseline_musteri, 0), 0)   AS baseline_ort_bakiye,
    /* --- KAMPANYA RATE --- */
    a.ort_kampanya_rate,
    a.kampanya_primi,
    /* --- BITIS (adet) --- */
    a.welcome_bitiyor_bu_hafta,
    a.welcome_bitiyor_gelecek_hafta,
    a.kampanya_bitiyor_gelecek_hafta,
    /* --- BITIS (hacim, TL) --- */
    a.welcome_bitiyor_bu_hafta_hacim,
    a.welcome_bitiyor_gelecek_hafta_hacim,
    a.kampanya_bitiyor_gelecek_hafta_hacim,
    /* --- BASLANGIC --- */
    a.kampanya_basliyor_bu_hafta,
    a.kampanya_basliyor_bu_hafta_hacim,
    /* --- DEGISIM (WoW: bu hafta - gecen hafta) --- */
    a.toplam_musteri     - LAG(a.toplam_musteri)     OVER (ORDER BY a.HAFTA_MIN) AS toplam_musteri_degisim,
    a.kampanyali_musteri - LAG(a.kampanyali_musteri) OVER (ORDER BY a.HAFTA_MIN) AS kampanyali_musteri_degisim,
    a.toplam_hacim       - LAG(a.toplam_hacim)       OVER (ORDER BY a.HAFTA_MIN) AS toplam_hacim_degisim,
    a.kampanyali_hacim   - LAG(a.kampanyali_hacim)   OVER (ORDER BY a.HAFTA_MIN) AS kampanyali_hacim_degisim
FROM wk_all a
ORDER BY a.HAFTA_MIN;

