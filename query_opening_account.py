query_opening_account = '''
/* ================================================================
  Turuncu hesap acilis analizi - Pazartesi-Cuma haftalik periyotlar.
  Bonus baslangic tarihi = rapor tarihi olan kayitlar uzerinden
  NTB (yeni musteri) ve EXISTING (mevcut musteri) ayrimini yapar.
  Haftasonu acilan hesaplar o haftanin Pzt-Cuma grubuna dahil edilir.
  Hafta kurgusu:
    Hafta Basi = Pazartesi  (WEEK_START)
    Hafta Sonu = Cuma       (WEEK_END)
    Ilk Hafta  = 30.09.2024 - 04.10.2024
    Referans   = 30.09.2024 (Pazartesi)
================================================================ */
WITH base_th AS (
  /* -- Musteri bazinda gunluk ozet
     Bonus baslangic tarihi = rapor tarihi filtresiyle
     sadece hesap acilis gunu kayitlari alinir. -- */
  SELECT
      t1.RAPOR_TARIHI,
      t1.BONUS_BAS_TARIHI,
      t1.MUSTERI_NO
  FROM PRSN.TURUNCU_YENI t1
  WHERE t1.RAPOR_TARIHI >= DATE '2024-09-30'
    AND t1.CALISMA_SEKLI = 1
    AND t1.PARA_KODU = 0
    AND t1.BONUS_BAS_TARIHI = t1.RAPOR_TARIHI
  GROUP BY
      t1.RAPOR_TARIHI,
      t1.BONUS_BAS_TARIHI,
      t1.MUSTERI_NO
),
/* -- NTB / EXISTING flag
   Musteri master tablosundaki acilis tarihiyle bonus baslangic
   tarihi arasinda 7 gun veya daha az fark varsa NTB (yeni musteri),
   degilse EXISTING (mevcut musteri) olarak isaretlenir. -- */
with_ntb_flag AS (
  SELECT
      b.RAPOR_TARIHI,
      b.MUSTERI_NO,
      CASE
          WHEN m.ACILIS_TARIHI IS NOT NULL
           AND ABS(TRUNC(b.BONUS_BAS_TARIHI) - TRUNC(m.ACILIS_TARIHI)) <= 7
          THEN 'NTB'
          ELSE 'EXISTING'
      END AS NTB_F
  FROM base_th b
  LEFT JOIN OPR.V_FS_MUSTERI_MASTER m
      ON b.MUSTERI_NO = m.MUSTERI_NO
),
/* -- Pazartesi-Cuma hafta etiketi (30.09.2024 referans Pazartesi)
   MOD ile her rapor tarihini haftanin Pazartesisine ceker,
   +4 ile Cumayi hesaplar.
   Haftasonu acilan hesaplar da o haftanin grubuna duser. -- */
weekly_labeled AS (
  SELECT
      TRUNC(RAPOR_TARIHI)
          - MOD(TRUNC(RAPOR_TARIHI) - DATE '2024-09-30', 7)
      AS WEEK_START,
      TRUNC(RAPOR_TARIHI)
          - MOD(TRUNC(RAPOR_TARIHI) - DATE '2024-09-30', 7) + 4
      AS WEEK_END,
      NTB_F,
      MUSTERI_NO
  FROM with_ntb_flag
  WHERE TRUNC(RAPOR_TARIHI) <= TRUNC(SYSDATE)
)
/* -- Final: Haftalik NTB vs EXISTING ve toplam acilan hesap -- */
SELECT
  TO_CHAR(WEEK_START, 'DD/MM/YYYY') AS MIN_RAPOR_TARIHI,
  TO_CHAR(WEEK_END,   'DD/MM/YYYY') AS MAX_RAPOR_TARIHI,
  COUNT(DISTINCT CASE WHEN NTB_F = 'NTB'      THEN MUSTERI_NO END) AS TOPLAM_NTB,
  COUNT(DISTINCT CASE WHEN NTB_F = 'EXISTING' THEN MUSTERI_NO END) AS TOPLAM_EXISTING,
  COUNT(DISTINCT MUSTERI_NO) AS OPENING_ACCOUNT
FROM weekly_labeled
GROUP BY WEEK_START, WEEK_END
ORDER BY WEEK_START
'''
