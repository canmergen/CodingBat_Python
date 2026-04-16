query_osa = '''
WITH params AS (
  SELECT DATE '2024-09-23' AS baslangic_tarihi FROM dual
),
/* -- OSA Hosgeldin faiz araliklari -- */
osa_welcome AS (
  SELECT
      t1.start_date,
      t1.end_date,
      t1.rate                                               AS OSAWelcome,
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
/* -- Turuncu gunluk - Pazartesi-Cuma hafta etiketi
   Haftasonu kayitlari filtrelenir -- */
turuncu_gunluk AS (
  SELECT
      t1.RAPOR_TARIHI,
      /* Pazartesi-Cuma hafta basi (Pazartesi = 23.09.2024 referansi) */
      t1.RAPOR_TARIHI
          - MOD(t1.RAPOR_TARIHI - DATE '2024-09-23', 7)
      AS HAFTA_BASLANGIC,
      t1.RAPOR_TARIHI
          - MOD(t1.RAPOR_TARIHI - DATE '2024-09-23', 7) + 4
      AS HAFTA_BITIS,
      SUM(t1.VADELI_BAKIYE)                                     AS SUM_VADELI,
      SUM(t1.VADESIZ_BAKIYE)                                    AS SUM_VADESIZ,
      SUM(t1.VADELI_BAKIYE_TL)                                  AS SUM_VADELI_TL,
      SUM(t1.TH_TOTAL_BAKIYE)                                   AS SUM_TH_TOTAL,
      SUM(t1.VADELI_BAKIYE * t1.VADELI_SPREAD)
          / NULLIF(SUM(t1.VADELI_BAKIYE), 0)                    AS VDELI_SPREAD,
      SUM(t1.VDSZ_DHL_SPREAD * t1.TH_TOTAL_BAKIYE)
          / NULLIF(SUM(t1.TH_TOTAL_BAKIYE), 0)                  AS VDESIZ_DAHIL_SPREAD,
      SUM(t1.VADELI_BAKIYE * t1.FAIZ_ORAN)
          / NULLIF(SUM(t1.VADELI_BAKIYE), 0)                    AS VADELI_COF
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
  ) t1
  GROUP BY
      t1.RAPOR_TARIHI,
      t1.RAPOR_TARIHI - MOD(t1.RAPOR_TARIHI - DATE '2024-09-23', 7),
      t1.RAPOR_TARIHI - MOD(t1.RAPOR_TARIHI - DATE '2024-09-23', 7) + 4
),
/* -- Gunluk -> Haftalik agregasyon -- */
turuncu_haftalik AS (
  SELECT
      HAFTA_BASLANGIC                                        AS MIN_TARIH,
      HAFTA_BITIS                                            AS MAX_TARIH,
      COUNT(DISTINCT RAPOR_TARIHI)                           AS IS_GUNU,
      AVG(SUM_VADELI)                                        AS AVG_VADELI_BAKIYE,
      AVG(SUM_VADESIZ)                                       AS AVG_VADESIZ_BAKIYE,
      AVG(SUM_VADELI_TL)                                     AS AVG_VADELI_TL,
      AVG(SUM_TH_TOTAL)                                      AS AVG_TH_TOTAL,
      SUM(SUM_VADELI * VADELI_COF)
          / NULLIF(SUM(SUM_VADELI), 0)                       AS VADELI_COF_HAFTA,
      (POWER(1 + (SUM(SUM_VADELI * VADELI_COF)
                  / NULLIF(SUM(SUM_VADELI), 0)) / 36500,
             365) - 1) * 100                                 AS OSABook,
      SUM(SUM_VADELI * VADELI_COF)
          / NULLIF(SUM(SUM_TH_TOTAL), 0)                     AS TOPLAM_COF_BASIT,
      AVG(VDELI_SPREAD)                                      AS AVG_VDELI_SPREAD,
      AVG(VDESIZ_DAHIL_SPREAD)                               AS AVG_VDESIZ_DAHIL_SPREAD
  FROM turuncu_gunluk
  GROUP BY HAFTA_BASLANGIC, HAFTA_BITIS
)
/* -- Final cikti -- */
SELECT
  h.MIN_TARIH,
  h.MAX_TARIH,
  h.IS_GUNU,
  h.VADELI_COF_HAFTA                                         AS OSABook_Basit,
  h.OSABook,
  h.TOPLAM_COF_BASIT,
  h.AVG_VADELI_BAKIYE,
  h.AVG_VADESIZ_BAKIYE,
  h.AVG_VADELI_TL,
  h.AVG_TH_TOTAL,
  h.AVG_VDELI_SPREAD,
  h.AVG_VDESIZ_DAHIL_SPREAD,
  w.OSAWelcome,
  w.OSAWelcomeAnnual,
  w.OSAWelcomeAnnualwCurrent,
  AVG(f.OSAFTP)                                              AS OSAFTP,
  h.TOPLAM_COF_BASIT / NULLIF(w.OSAWelcome, 0)              AS MALIYET_HOSGELDIN_ORANI
FROM turuncu_haftalik h
LEFT JOIN osa_welcome w
  ON h.MIN_TARIH BETWEEN w.start_date AND w.end_date
LEFT JOIN osa_ftp f
  ON f.TANIMTARIH BETWEEN h.MIN_TARIH AND h.MAX_TARIH
GROUP BY
  h.MIN_TARIH, h.MAX_TARIH, h.IS_GUNU,
  h.VADELI_COF_HAFTA, h.OSABook, h.TOPLAM_COF_BASIT,
  h.AVG_VADELI_BAKIYE, h.AVG_VADESIZ_BAKIYE, h.AVG_VADELI_TL,
  h.AVG_TH_TOTAL, h.AVG_VDELI_SPREAD, h.AVG_VDESIZ_DAHIL_SPREAD,
  w.OSAWelcome, w.OSAWelcomeAnnual, w.OSAWelcomeAnnualwCurrent
ORDER BY h.MIN_TARIH
'''
