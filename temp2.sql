/* ================================================================
   OSA Book haftalik agregasyon — SADECE STANDART OSAWELCOME MUSTERILERI
   (baseline; netflow_pure_osawelcome.sql ile AYNI populasyon).

   osabook_weekly.sql ile BIREBIR ayni mantik; TEK EK:
   - Her musteri-gun satirina o tarihte gecerli STANDART osawelcome
     baglanir (osa_welcome: OPR.V_FS_SVIN_DEPOSIT_INTEREST,
     interest_code=1, currency_code=0).
   - Musterinin KAMPANYA_FAIZ_ORANI'si o gunku standart osawelcome'dan
     FAZLA ise (= ona ekstra/kampanya rate verilmis) o musteri-gun
     kaydi agregasyondan CIKARILIR.

   NETFLOW'DAN FARKLI olarak burada akis (iki uc) yok; her satir tek
   bir gunun snapshot'i. Bu yuzden TEK eslesmeyle (RAPOR_TARIHI) anti-
   join yeterli; filtre en icteki subquery'de (SUM'lardan ONCE)
   uygulanir.

   Cikarma (musteri, gun) bazinda: bir musteri 5-6 hafta yuksek rate
   alip sonra normale donerse, sadece yuksek-rate gunlerinde cikar,
   sonrasinda otomatik geri girer (kalici kara liste YOK).

   >>> DOGRULANACAK 2 SEY (netflow ile ayni):
   1) KAMPANYA_FAIZ_ORANI: TURUNCU_YENI'deki gercek kolon adiyla degistir.
   2) OLCEK: KAMPANYA_FAIZ_ORANI ile osa_welcome.rate AYNI bazda olmali
      (ham basit oran, orn. 40 / 43). Farkli birimde ise once normalize et.

   NaN/NULL kampanya orani => standart kabul, KORUNUR.
   Kampanya = osawelcome     => standart, KORUNUR.
   Kampanya > osawelcome     => EKSTRA, CIKARILIR.
   (Kampanya < osawelcome    => "fazla" degil; KORUNUR.)

   --- COF / OSAWelcome metodolojisi (degismedi) ---
   COF zinciri (OSABook_Basit, OSABook, TOPLAM_COF_BASIT,
   MALIYET_HOSGELDIN_ORANI) ve OSAWelcome turevleri haftanin SON IS
   GUNUNE (Cuma; tatilde son mevcut gun) gore alinir — haftalik AVG degil.
   Bakiye / spread / OSAFTP haftalik AVG kalir.
================================================================ */
WITH
params AS (
    SELECT DATE '2024-09-16' AS baslangic_tarihi FROM dual
),

/* -- OSA Hosgeldin faiz araliklari -- */
osa_welcome AS (
    SELECT
        t1.start_date,
        t1.end_date,
        t1.rate                                              AS OSAWelcome,
        (POWER(1 + t1.rate / 36500, 365) - 1) * 100          AS OSAWelcomeAnnual,
        (POWER(1 + (t1.rate / 36500) * 0.9, 365) - 1) * 100  AS OSAWelcomeAnnualwCurrent
    FROM OPR.V_FS_SVIN_DEPOSIT_INTEREST t1
    WHERE t1.deleted       = 0
      AND t1.interest_code = 1
      AND t1.currency_code = 0
),

/* -- OSA FTP orani (gunluk) -- */
osa_ftp AS (
    SELECT
        t1.TANIMTARIH,
        t1.D1ORAN AS OSAFTP
    FROM OPR.V_FS_FAIZPARAM t1
    WHERE t1.URUN    = 107
      AND t1.ALTURUN = '2'
      AND t1.PARA    = '0'
),

/* -- EKSTRA musteri-gun kayitlari: kampanya orani o gunku standart
   osawelcome'dan FAZLA. Bunlar agregasyondan cikarilacak.
   (netflow_pure_osawelcome.sql CTE_EXTRA ile AYNI tanim:
    PARA_KODU=0, CALISMA_SEKLI=1, kampanya > welcome.)
   << KAMPANYA_FAIZ_ORANI kolon adini DOGRULA >> -- */
cte_extra AS (
    SELECT /*+ MATERIALIZE */ DISTINCT
        t.RAPOR_TARIHI,
        t.MUSTERI_NO
    FROM PRSN.TURUNCU_YENI t
    CROSS JOIN params p
    JOIN osa_welcome w
      ON t.RAPOR_TARIHI BETWEEN w.start_date AND w.end_date
    WHERE t.CALISMA_SEKLI       = 1
      AND t.RAPOR_TARIHI       >= p.baslangic_tarihi
      AND t.PARA_KODU           = 0
      AND t.KAMPANYA_FAIZ_ORANI IS NOT NULL
      AND t.KAMPANYA_FAIZ_ORANI > w.OSAWelcome + 0.0001   -- float gurultusu icin kucuk tolerans
),

/* -- Turuncu gunluk: musteri-bazlilik kalktiktan sonraki gunluk agregalar
   Hafta etiketi (Pzt-Cum); haftasonu kayitlari haric
   << EKSTRA musteri-gun satirlari icteki WHERE'de NOT EXISTS ile cikar >> -- */
turuncu_gunluk AS (
    SELECT
        t1.RAPOR_TARIHI,
        /* Pazartesi-Cuma hafta basi (Pazartesi = 23.09.2024 referansi) */
        t1.RAPOR_TARIHI
            - MOD(t1.RAPOR_TARIHI - DATE '2024-09-23', 7)         AS HAFTA_BASLANGIC,
        t1.RAPOR_TARIHI
            - MOD(t1.RAPOR_TARIHI - DATE '2024-09-23', 7) + 4     AS HAFTA_BITIS,
        SUM(t1.VADELI_BAKIYE)                                     AS SUM_VADELI,
        SUM(t1.VADESIZ_BAKIYE)                                    AS SUM_VADESIZ,
        SUM(t1.VADELI_BAKIYE_TL)                                  AS SUM_VADELI_TL,
        SUM(t1.TH_TOTAL_BAKIYE)                                   AS SUM_TH_TOTAL,
        SUM(t1.VADELI_BAKIYE * t1.VADELI_SPREAD)
            / NULLIF(SUM(t1.VADELI_BAKIYE), 0)                    AS VDELI_SPREAD,
        SUM(t1.VDSZ_DHL_SPREAD * t1.TH_TOTAL_BAKIYE)
            / NULLIF(SUM(t1.TH_TOTAL_BAKIYE), 0)                  AS VDESIZ_DAHIL_SPREAD,
        SUM(t1.VADELI_BAKIYE * t1.FAIZ_ORAN)
            / NULLIF(SUM(t1.VADELI_BAKIYE), 0)                    AS VADELI_COF,
        /* DAILY OSABook — gunluk COF'u annualize et (calisan sorgudaki ile birebir) */
        (POWER(1 + (SUM(t1.VADELI_BAKIYE * t1.FAIZ_ORAN)
                    / NULLIF(SUM(t1.VADELI_BAKIYE), 0)) / 36500,
               365) - 1) * 100                                    AS DAILY_OSABook,
        /* Gunluk maliyet/hosgeldin payi (hosgeldin dis joinde ekleniyor) */
        SUM(t1.VADELI_BAKIYE * t1.FAIZ_ORAN)
            / NULLIF(SUM(t1.TH_TOTAL_BAKIYE), 0)                  AS DAILY_TOPLAM_COF_BASIT
    FROM (
        SELECT
            t1.RAPOR_TARIHI,
            t1.VADESIZ_BAKIYE,
            t1.VADELI_BAKIYE_TL,
            t1.VADELI_BAKIYE,
            t1.TH_TOTAL_BAKIYE,
            t1.VDSZ_DHL_SPREAD,
            t1.VADELI_SPREAD,
            t1.HAZINE_ORAN,
            t1.FAIZ_ORAN
        FROM PRSN.TURUNCU_YENI t1
        CROSS JOIN params p
        WHERE t1.CALISMA_SEKLI    = 1
          AND t1.RAPOR_TARIHI    >= p.baslangic_tarihi
          AND t1.TH_TOTAL_BAKIYE  > 0
          AND t1.URUN_KODU       <> 417
          AND t1.PARA_KODU        = 0
          AND TO_CHAR(t1.RAPOR_TARIHI, 'DY', 'NLS_DATE_LANGUAGE=AMERICAN') NOT IN ('SAT','SUN')
          /* <<< EKSTRA (ustu-welcome) musteri-gun kayitlarini AGREGASYONDAN CIKAR.
                 Tek tarih (snapshot) oldugu icin tek eslesme yeterli.
                 >>> OVERLAY istiyorsan: NOT EXISTS -> EXISTS yap. <<< */
          AND NOT EXISTS (
              SELECT 1
              FROM cte_extra e
              WHERE e.MUSTERI_NO   = t1.MUSTERI_NO
                AND e.RAPOR_TARIHI = t1.RAPOR_TARIHI
          )
    ) t1
    GROUP BY
        t1.RAPOR_TARIHI,
        t1.RAPOR_TARIHI - MOD(t1.RAPOR_TARIHI - DATE '2024-09-23', 7),
        t1.RAPOR_TARIHI - MOD(t1.RAPOR_TARIHI - DATE '2024-09-23', 7) + 4
),

/* -- Gunluk seviyede OSAWelcome + OSAFTP ile zenginlestir
   Hafta-ici rate degisikliginde her gun kendi degerine matchlenir -- */
turuncu_gunluk_enriched AS (
    SELECT
        g.RAPOR_TARIHI,
        g.HAFTA_BASLANGIC,
        g.HAFTA_BITIS,
        g.SUM_VADELI,
        g.SUM_VADESIZ,
        g.SUM_VADELI_TL,
        g.SUM_TH_TOTAL,
        g.VDELI_SPREAD,
        g.VDESIZ_DAHIL_SPREAD,
        g.VADELI_COF,
        g.DAILY_OSABook,
        g.DAILY_TOPLAM_COF_BASIT,
        w.OSAWelcome                  AS DAILY_OSAWelcome,
        w.OSAWelcomeAnnual            AS DAILY_OSAWelcomeAnnual,
        w.OSAWelcomeAnnualwCurrent    AS DAILY_OSAWelcomeAnnualwCurrent,
        f.OSAFTP                      AS DAILY_OSAFTP,
        g.DAILY_TOPLAM_COF_BASIT
            / NULLIF(w.OSAWelcome, 0)  AS DAILY_MALIYET_HOSGELDIN_ORANI
    FROM turuncu_gunluk g
    LEFT JOIN osa_welcome w
        ON g.RAPOR_TARIHI BETWEEN w.start_date AND w.end_date
    LEFT JOIN osa_ftp f
        ON f.TANIMTARIH = g.RAPOR_TARIHI
),

/* -- Gunluk -> Haftalik agregasyon:
   COF zinciri = haftanin son is gunu (KEEP DENSE_RANK LAST)
   Diger metrikler = haftalik AVG -- */
turuncu_haftalik AS (
    SELECT
        HAFTA_BASLANGIC                                                          AS MIN_TARIH,
        HAFTA_BITIS                                                              AS MAX_TARIH,
        COUNT(DISTINCT RAPOR_TARIHI)                                             AS IS_GUNU,
        AVG(SUM_VADELI)                                                          AS AVG_VADELI_BAKIYE,
        AVG(SUM_VADESIZ)                                                         AS AVG_VADESIZ_BAKIYE,
        AVG(SUM_VADELI_TL)                                                       AS AVG_VADELI_TL,
        AVG(SUM_TH_TOTAL)                                                        AS AVG_TH_TOTAL,
        /* COF zinciri: haftanin son is gununun (Cuma; tatilde son mevcut gun) degeri */
        MAX(VADELI_COF)              KEEP (DENSE_RANK LAST ORDER BY RAPOR_TARIHI) AS OSABook_Basit,
        MAX(DAILY_OSABook)           KEEP (DENSE_RANK LAST ORDER BY RAPOR_TARIHI) AS OSABook,
        MAX(DAILY_TOPLAM_COF_BASIT)  KEEP (DENSE_RANK LAST ORDER BY RAPOR_TARIHI) AS TOPLAM_COF_BASIT,
        MAX(DAILY_MALIYET_HOSGELDIN_ORANI)
                                     KEEP (DENSE_RANK LAST ORDER BY RAPOR_TARIHI) AS MALIYET_HOSGELDIN_ORANI,
        AVG(VDELI_SPREAD)                                                        AS AVG_VDELI_SPREAD,
        AVG(VDESIZ_DAHIL_SPREAD)                                                 AS AVG_VDESIZ_DAHIL_SPREAD,
        /* OSAWelcome zinciri: haftanin son is gununun (Cuma; tatilde son mevcut gun) degeri */
        MAX(DAILY_OSAWelcome)              KEEP (DENSE_RANK LAST ORDER BY RAPOR_TARIHI) AS OSAWelcome,
        MAX(DAILY_OSAWelcomeAnnual)        KEEP (DENSE_RANK LAST ORDER BY RAPOR_TARIHI) AS OSAWelcomeAnnual,
        MAX(DAILY_OSAWelcomeAnnualwCurrent) KEEP (DENSE_RANK LAST ORDER BY RAPOR_TARIHI) AS OSAWelcomeAnnualwCurrent,
        AVG(DAILY_OSAFTP)                                                        AS OSAFTP
    FROM turuncu_gunluk_enriched
    GROUP BY HAFTA_BASLANGIC, HAFTA_BITIS
)

/* -- Final cikti -- */
SELECT
    MIN_TARIH,
    MAX_TARIH,
    IS_GUNU,
    OSABook_Basit,
    OSABook,
    TOPLAM_COF_BASIT,
    AVG_VADELI_BAKIYE,
    AVG_VADESIZ_BAKIYE,
    AVG_VADELI_TL,
    AVG_TH_TOTAL,
    AVG_VDELI_SPREAD,
    AVG_VDESIZ_DAHIL_SPREAD,
    OSAWelcome,
    OSAWelcomeAnnual,
    OSAWelcomeAnnualwCurrent,
    OSAFTP,
    MALIYET_HOSGELDIN_ORANI
FROM turuncu_haftalik
ORDER BY MIN_TARIH
