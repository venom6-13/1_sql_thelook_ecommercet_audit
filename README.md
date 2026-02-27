# 1_sql_thelook_ecommerce
# TheLook E-Commerce : Profitability & Return Rate Audit 📊

> **Stack :** BigQuery SQL · Google Sheets  
> **Dataset :** `bigquery-public-data.thelook_ecommerce`  
> **Période :** Janvier 2025 – Février 2026 (14 mois)  
> **Catégorie :** Fashion Hoodies & Sweatshirts

---

## 1. 🎯 Problématique Métier

La direction de **TheLook** observe une anomalie : malgré une croissance du chiffre d'affaires
de **+163% en 14 mois**, la rentabilité nette ne progresse pas.
Certains segments génèrent un fort volume de ventes mais une marge faible —
ils "brassent de l'air" — tandis que les taux de retour restent élevés et érodent
silencieusement le P&L.

**Questions business :**
- Quels segments (géo, genre, canal) génèrent du volume sans rentabilité réelle ?
- Quel est l'impact financier concret des retours sur la marge nette ?
- Quelles actions permettraient de réduire les coûts logistiques de manière durable ?

---

## 2. 🛠️ Exploration & SQL

### Architecture : 1 CTE par dimension métier

```sql
declare date_debut date default '2025-01-01';
declare date_fin   date default current_date();

with order_items_base as (
  -- Source unique de vérité : réconciliation des 4 tables
  select
    oi.order_id,
    oi.product_id,
    oi.sale_price,
    p.cost,
    oi.status,
    o.delivered_at,
    u.country,
    u.gender,
    u.traffic_source
  from `bigquery-public-data.thelook_ecommerce.order_items` oi
  left join `bigquery-public-data.thelook_ecommerce.products`  p on p.id        = oi.product_id
  left join `bigquery-public-data.thelook_ecommerce.orders`    o on o.order_id  = oi.order_id
  left join `bigquery-public-data.thelook_ecommerce.users`     u on u.id        = o.user_id
  where oi.status in ('Complete', 'Returned')
    and date(o.delivered_at) between date_debut and date_fin
    and p.category = 'Fashion Hoodies & Sweatshirts'
),

financial_monthly as (
  select
    extract(year  from delivered_at)  as annee,
    extract(month from delivered_at)  as mois,
    country,
    count(distinct order_id)                                           as nb_commandes,
    count(*)                                                           as nb_articles,
    round(sum(sale_price), 2)                                          as chiffre_affaires,
    round(safe_divide(
      sum(sale_price - cost), sum(sale_price)) * 100, 2)               as taux_marge_brute_pct,
    round(safe_divide(
      sum(sale_price - cost)
        - countif(status = 'Returned') * avg(sale_price) * 0.15,
      sum(sale_price)) * 100, 2)                                       as taux_marge_nette_pct,
    round(safe_divide(
      countif(status = 'Returned'), count(*)) * 100, 2)                as return_rate_pct,
    round(safe_divide(sum(sale_price), count(distinct order_id)), 2)   as aov
  from order_items_base
  group by 1, 2, 3
),

sessions_monthly as (
  select
    extract(year  from created_at) as annee,
    extract(month from created_at) as mois,
    count(distinct session_id)     as total_sessions,
    countif(event_type = 'purchase') as total_purchases
  from `bigquery-public-data.thelook_ecommerce.events`
  where date(created_at) between date_debut and date_fin
  group by 1, 2
),

retention_monthly as (
  select
    extract(year  from o.delivered_at) as annee,
    extract(month from o.delivered_at) as mois,
    round(safe_divide(
      countif(order_rank > 1),
      count(*)) * 100, 2)              as taux_retention_pct
  from (
    select
      user_id,
      delivered_at,
      row_number() over (partition by user_id order by delivered_at) as order_rank
    from `bigquery-public-data.thelook_ecommerce.orders`
    where date(delivered_at) between date_debut and date_fin
  ) o
  group by 1, 2
)

select
  f.*,
  round(safe_divide(s.total_purchases, s.total_sessions) * 100, 2) as conversion_rate_pct,
  r.taux_retention_pct
from financial_monthly  f
left join sessions_monthly  s using (annee, mois)
left join retention_monthly r using (annee, mois)
order by f.annee, f.mois, f.country;
```

> Code complet → [`/sql/01_main_monthly.sql`](sql/01_main_monthly.sql)

---

### Justifications techniques

**Pourquoi `LEFT JOIN` plutôt que `INNER JOIN` ?**
```sql
left join `...products` p on p.id = oi.product_id
left join `...users`    u on u.id = o.user_id
```
> Un `INNER JOIN` aurait silencieusement exclu les articles dont les métadonnées
> sont manquantes. Dans un audit de rentabilité, chaque article vendu doit être
> comptabilisé — même sans données enrichies. Le `LEFT JOIN` garantit l'exhaustivité.

**Pourquoi `SAFE_DIVIDE` ?**
```sql
round(safe_divide(sum(marge), sum(chiffre_affaires)) * 100, 2)
```
> En cas de mois sans vente, l'opérateur `/` lève une erreur `Division by zero`
> et interrompt le pipeline. `SAFE_DIVIDE` retourne `NULL` — le rapport reste
> lisible et robuste pour un usage en production.

**Pourquoi `delivered_at` comme date de référence ?**
> `created_at` inclurait des commandes encore annulables ou en transit.
> `delivered_at` garantit que seules les transactions **livrées et confirmées**
> entrent dans le calcul de rentabilité — base correcte pour un audit P&L.

**Pourquoi une marge nette estimée en plus de la marge brute ?**
```sql
sum(sale_price - cost)
  - countif(status = 'Returned') * avg(sale_price) * 0.15
```
> La marge brute ne reflète pas la réalité opérationnelle. Chaque retour génère
> des coûts logistiques inverses : transport retour, traitement, reconditionnement.

> ⚠️ **Note méthodologique :** Le dataset TheLook ne contient pas les coûts
> logistiques réels des retours. Le taux de **15%** est une estimation basée sur
> les benchmarks industrie : NRF (16.9%), Zalando (12–15%). Quelle que soit
> l'hypothèse retenue (10–20%), la conclusion est inchangée : **3.5 à 4.5 points
> de marge nette sont érodés par les retours**.

---

### Référentiel des seuils utilisés

| Métrique | Excellent | Acceptable | Préoccupant | Critique |
|---|---|---|---|---|
| Return Rate | < 15% | 15–25% | 25–35% | > 35% |
| Marge Brute | > 55% | 45–55% | 35–45% | < 35% |
| Taux de Rebond | < 20% | 20–35% | 35–50% | > 50% |

---

## 3. 💡 Insights & Recommandations

---

### Insight #1 — Les retours coûtent cher, et ça ne s'améliore pas

**Ce que j'ai vu**

| Période | Return Rate | Marge Brute | Marge Nette | Coût Retours |
|---|---|---|---|---|
| Jan 2025 | 29.5% 🟠 | 48.4% | 44.0% | ~$145 |
| Moy. Jan–Oct 2025 | ~28.5% 🟠 | ~48% | ~43.8% | ~$250/mois |
| Nov–Déc 2025 | ~19% 🟡 | ~48% | ~45.2% | ~$170/mois |
| Jan–Fév 2026 | ~30% 🟠 | ~48% | ~43.8% | ~$535/mois |
| **Total période** | **~28% 🟠** | **~48%** | **~44%** | **~$4 290** |

**Ce que j'en déduis**
La croissance de +163% du CA n'a pas réduit le return rate d'un seul point.
Plus on vend, plus on retourne — dans les mêmes proportions. Ça suggère un problème
de fiche produit : les clients reçoivent quelque chose qui ne correspond pas à leurs
attentes (taille, description, photos).

**Ma recommandation**
Identifier les 3 SKUs avec le return rate le plus élevé et améliorer leurs fiches
(guide des tailles précis, vraies photos portées). Seuil d'alerte : tout produit
affichant un return rate > 35% déclenche une révision avant toute nouvelle campagne paid.

---

### Insight #2 — Les femmes rapportent +8 pts de marge, mais on vend surtout aux hommes

**Ce que j'ai vu**

| Segment | Marge Brute Moy. | Part du CA | Croissance | Return Rate |
|---|---|---|---|---|
| 👩 Femmes | **53% 🟢** | 40% | +153% | ~28% |
| 👨 Hommes | **45% 🟡** | 60% | +215% | ~27% |
| **Écart** | **8 points** | — | Hommes croissent + vite | — |

**Ce que j'en déduis**
Le business vend plus aux hommes, mais gagne plus d'argent par euro dépensé
par les femmes. Et les hommes croissent plus vite — ce qui signifie que la marge
globale se dégrade mécaniquement chaque mois, sans que rien ne change au produit.

**Ma recommandation**
Réallouer 30% du budget acquisition vers le segment féminin. Passer de 40% à 50%
de part féminine dans le CA = **+1.6 pts de marge globale** sans changer les coûts.

---

### Insight #3 — Le Q4 prouve qu'un return rate de 19% est atteignable — mais ça ne dure pas

**Ce que j'ai vu**

| Période | Return Rate | Marge Nette | Signal |
|---|---|---|---|
| Jan–Oct 2025 | ~28.5% 🟠 | ~43.8% | Niveau structurel |
| **Nov 2025** | **19.3% 🟡** | **44.5%** | ⬇️ -9 pts |
| **Déc 2025** | **18.8% 🟡** | **45.9% 🟡** | Record absolu |
| Jan 2026 | 29.5% 🟠 | 43.9% | Retour à la normale |

**Ce que j'en déduis**
En fin d'année, les acheteurs planifient leurs achats (cadeaux) et retournent moins.
En janvier, les soldes attirent des achats impulsifs qui finissent retournés.
La différence n'est pas dans le produit — elle est dans l'intention de l'acheteur.
Décembre 2025 prouve que **45.9% de marge nette est atteignable**.

**Ma recommandation**
Reproduire le modèle Q4 hors saison : contenu éditorial informatif plutôt que
promotionnel, ciblage d'audiences intentionnelles. Objectif : maintenir le return
rate sous 22% toute l'année — pas seulement en Q4.

---

### Insight #4 — Les campagnes Email de janvier font plus de mal que de bien

**Ce que j'ai vu**

| Mois | Email Return Rate | Tous canaux Return Rate |
|---|---|---|
| Jan 2025 | **100% ☠️** | 29.5% 🟠 |
| Fév–Déc 2025 | 0–37.5% 🟡 / 🟠 | 19–33% 🟡 / 🟠 |
| **Jan 2026** | **55.6% ☠️** | 29.5% 🟠 |

**Ce que j'en déduis**
Deux années consécutives, même canal, même mois, même résultat.
Les campagnes Email de janvier attirent des acheteurs post-fêtes impulsifs
qui retournent quasi-systématiquement. Le canal Email fonctionne bien le reste
de l'année — le problème est uniquement le timing de janvier.

**Ma recommandation**
Suspendre toutes les campagnes Email d'acquisition en janvier. Les remplacer par
des emails de **rétention** ciblant uniquement les clients existants sans historique
de retour (rappel produit, recommandation personnalisée).

---

### Insight #5 — La Chine représente 42% du CA avec un return rate qu'on ne maîtrise pas

**Ce que j'ai vu**

| Métrique | Chine | Moyenne Globale |
|---|---|---|
| Part du CA total | **42%** | — |
| Commandes jan 2025 | 30 | — |
| Commandes fév 2026 | 79 (+163%) | — |
| Return Rate moyen | **~28% 🟠** | ~28% 🟠 |
| Return Rate min | 18.4% 🟡 (sep 2025) | — |
| Return Rate max | 43.7% 🔴 (jan 2025) | — |
| Amélioration sur 14 mois | **Aucune** | — |

**Ce que j'en déduis**
42% du CA dans un seul marché est un risque de concentration. Et ce marché a un
return rate élevé et instable qu'on ne maîtrise pas. Cause probable : les standards
de taille occidentaux ne correspondent pas bien aux morphologies asiatiques.

**Ma recommandation**
Adapter les fiches produits pour le marché chinois en priorité : guide des tailles
en mandarin, correspondance avec les standards chinois. Chaque point de return rate
récupéré sur ce marché = **~$150/mois d'économie directe** à partir du volume actuel.

---

### Insight #6 — Deux marchés prouvent qu'on peut faire bien mieux sur les retours

**Ce que j'ai vu**

| Marché | Return Rate | vs Moyenne Globale | Durée |
|---|---|---|---|
| 🇯🇵 Japon | **~15% 🟢** | -13 pts | Constant sur 14 mois |
| 🇧🇷 Brésil (nov 2025) | **10.7% 🟢** | -17 pts | 1 mois |
| 🇧🇷 Brésil (déc 2025) | **4.6% 🟢** | -23 pts | Record absolu du dataset |
| 🇧🇷 Brésil (jan 2026) | 28.1% 🟠 | — | Retour à la normale |

**Ce que j'en déduis**
Ces deux cas prouvent qu'un return rate bien inférieur à 28% est possible —
ce n'est pas une utopie. Le Japon le montre structurellement. Le Brésil en Q4
le montre ponctuellement, sans qu'on ait identifié la cause.

**Ma recommandation**
- **Japon** : tester une activation (page produit en japonais, campagne Search ciblée).
  Si le comportement se confirme à plus grande échelle, c'est un marché prioritaire
  pour sa qualité, pas son volume.
- **Brésil Q4** : identifier ce qui a changé en nov-déc 2025 et le répliquer sur
  les autres marchés. C'est le blueprint anti-retours le plus précieux du dataset.

---

## 4. 📁 Structure du Repository

```
thelook-ecommerce-audit/
│
├── sql/
│   ├── 01_main_monthly.sql         # KPIs financiers par pays / mois (requête principale)
│   ├── 02_returns_impact.sql       # Impact des retours sur la marge nette
│   ├── 03_gender_margin.sql        # Segmentation femmes / hommes
│   ├── 04_seasonality.sql          # Saisonnalité Q4 — return rate & marge
│   ├── 05_email_january.sql        # Analyse canal Email en janvier
│   ├── 06_china_focus.sql          # Focus marché chinois
│   └── 07_japan_brazil.sql         # Marchés à fort potentiel anti-retours
│
│
└── README.md
```

---

## 6. 🧰 Stack Technique

![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=flat&logo=google-cloud&logoColor=white)
![Google Sheets](https://img.shields.io/badge/Google%20Sheets-34A853?style=flat&logo=google-sheets&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-CTE%20Architecture-blue?style=flat)

---

*Projet réalisé dans le cadre d'un programme de formation en Data Analytics.*
*Dataset : Google BigQuery Public Data — TheLook Ecommerce.*
