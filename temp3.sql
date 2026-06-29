/* ================================================================
   Turuncu hesap acilis analizi — SADECE STANDART OSAWELCOME MUSTERILERI
   (baseline; netflow_pure_osawelcome.sql / osabook_pure_osawelcome.sql
    ile AYNI populasyon).

   Orijinal opening_account sorgusu ile BIREBIR ayni mantik; TEK EK:
   - Acilis gunu (BONUS_BAS_TARIHI = RAPOR_TARIHI) o tarihte gecerli
     STANDART osawelcome ile kiyaslanir.
   - Musterinin KAMPANYA_FAIZ_ORANI'si o gunku standart osawelcome'dan
     FAZLA ise (= ona ekstra/kampanya rate verilmis) o acilis baseline
     sayimindan CIKARILIR (NTB / EXISTING / toplam — hepsinden).

   Tek tarih (acilis gunu snapshot) oldugu icin TEK eslesmeyle
   (RAPOR_TARIHI) anti-join yeterli; filtre GROUP BY'dan ONCE ham
   satirlarda uygulanir.

   >>> DOGRULANACAK 2 SEY (netflow/osabook ile ayni):
   1) KAMPANYA_FAIZ_ORANI: TURUNCU_YENI'deki gercek kolon adiyla degistir.
   2) OLCEK: KAMPANYA_FAIZ_ORANI ile osa_welcome.rate AYNI bazda olmali.

   --- Hafta kurgusu (degismedi) ---
     Hafta Basi = Pazartesi (WEEK_START), Hafta Sonu = Cuma (WEEK_END)
     Ilk Hafta  = 30.09.2024 - 04.10.2024, Referans = 30.09.2024 (Pzt)
   Haftasonu acilan hesaplar o haftanin Pzt-Cuma grubuna dahil edilir.
================================================================ */
WITH
/* -- OSA Hosgeldin faiz araliklari (ekstra-rate kiyasi icin) -- */
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

/* -- EKSTRA musteri-gun kayitlari: kampanya orani o gunku standart
   osawelcome'dan FAZLA. (netflow/osabook CTE_EXTRA ile AYNI tanim.)
   << KAMPANYA_FAIZ_ORANI kolon adini DOGRULA >> -- */
cte_extra AS (
    SELECT /*+ MATERIALIZE */ DISTINCT
        t.RAPOR_TARIHI,
        t.MUSTERI_NO
    FROM PRSN.TURUNCU_YENI t
    JOIN osa_welcome w
      ON t.RAPOR_TARIHI BETWEEN w.start_date AND w.end_date
    WHERE t.CALISMA_SEKLI       = 1
      AND t.RAPOR_TARIHI       >= DATE '2024-09-30'
      AND t.PARA_KODU           = 0
      AND t.KAMPANYA_FAIZ_ORANI IS NOT NULL
      AND t.KAMPANYA_FAIZ_ORANI > w.OSAWelcome + 0.0001   -- float gurultusu icin kucuk tolerans
),

/* -- Musteri bazinda gunluk ozet
   Bonus baslangic tarihi = rapor tarihi filtresiyle sadece hesap
   acilis gunu kayitlari alinir.
   << EKSTRA (ustu-welcome) musteriler acilis gununde NOT EXISTS ile cikar >> -- */
base_th AS (
    SELECT
        t1.RAPOR_TARIHI,
        t1.BONUS_BAS_TARIHI,
        t1.MUSTERI_NO
    FROM PRSN.TURUNCU_YENI t1
    WHERE t1.RAPOR_TARIHI    >= DATE '2024-09-30'
      AND t1.CALISMA_SEKLI    = 1
      AND t1.PARA_KODU        = 0
      AND t1.BONUS_BAS_TARIHI = t1.RAPOR_TARIHI
      /* <<< EKSTRA musteri-gun kayitlarini AKISTAN CIKAR.
             >>> OVERLAY istiyorsan: NOT EXISTS -> EXISTS yap. <<< */
      AND NOT EXISTS (
          SELECT 1
          FROM cte_extra e
          WHERE e.MUSTERI_NO   = t1.MUSTERI_NO
            AND e.RAPOR_TARIHI = t1.RAPOR_TARIHI
      )
    GROUP BY
        t1.RAPOR_TARIHI,
        t1.BONUS_BAS_TARIHI,
        t1.MUSTERI_NO
),

/* -- NTB / EXISTING flag
   Musteri master tablosundaki acilis tarihiyle bonus baslangic tarihi
   arasinda 7 gun veya daha az fark varsa NTB (yeni musteri),
   degilse EXISTING (mevcut musteri). -- */
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
   Haftasonu acilan hesaplar da o haftanin grubuna duser. -- */
weekly_labeled AS (
    SELECT
        TRUNC(RAPOR_TARIHI)
            - MOD(TRUNC(RAPOR_TARIHI) - DATE '2024-09-30', 7)       AS WEEK_START,
        TRUNC(RAPOR_TARIHI)
            - MOD(TRUNC(RAPOR_TARIHI) - DATE '2024-09-30', 7) + 4   AS WEEK_END,
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
    COUNT(DISTINCT MUSTERI_NO)                                       AS OPENING_ACCOUNT
FROM weekly_labeled
GROUP BY WEEK_START, WEEK_END
ORDER BY WEEK_START
