declare date_debut date default '2025-01-01';
declare date_fin   date default current_date();

--Base complète
with order_items_base as (
  select
    p.category,
    p.brand,
    oi.order_id,
    oi.sale_price,
    p.cost,
    oi.status,
    oi.delivered_at,
    u.country,
    u.gender,
    u.traffic_source

  from `bigquery-public-data.thelook_ecommerce.order_items` oi
  left join `bigquery-public-data.thelook_ecommerce.products` p
    on p.id = oi.product_id
  left join `bigquery-public-data.thelook_ecommerce.orders` o
    on o.order_id = oi.order_id
  left join `bigquery-public-data.thelook_ecommerce.users` u
    on u.id = o.user_id

  where date(oi.delivered_at) between date_debut and date_fin
    and p.category = 'Fashion Hoodies & Sweatshirts'
),

-- Perf. financière par mois
financial_monthly as (
  select
    extract(year  from delivered_at) as annee,
    extract(month from delivered_at) as mois,
    category,
    country,

    count(distinct order_id) as nb_commandes,
    count(*)                 as nb_articles_vendus,

    round(sum(sale_price), 2)        as chiffre_affaires,
    round(sum(cost), 2)              as cout_total,
    round(sum(sale_price - cost), 2) as marge,

    round(
      safe_divide(sum(sale_price), count(distinct order_id)),
      2
    ) as aov,

    -- Marge brute
    round(
      safe_divide(sum(sale_price - cost), sum(sale_price)) * 100,
      2
    ) as taux_marge_brute_pct,

    -- Marge nette estimée : hypothèse coût logistique retour = 15% du prix de vente
    round(
      safe_divide(
        sum(sale_price - cost) - countif(status = 'Returned') * avg(sale_price) * 0.15,
        sum(sale_price)
      ) * 100,
      2
    ) as taux_marge_nette_estimee_pct,

    round(
      safe_divide(countif(status = 'Returned'), count(*)) * 100,
      2
    ) as return_rate_pct

  from order_items_base
  group by annee, mois, category, country
),

-- Acquisition
sessions_monthly as (
  select
    extract(year  from created_at) as annee,
    extract(month from created_at) as mois,
    count(distinct session_id)       as total_sessions,
    countif(event_type = 'purchase') as total_orders
  from `bigquery-public-data.thelook_ecommerce.events`
  where date(created_at) between date_debut and date_fin
  group by annee, mois
),

conversion_monthly as (
  select
    annee,
    mois,
    round(
      safe_divide(total_orders, total_sessions) * 100,
      2
    ) as conversion_rate_pct
  from sessions_monthly
),

bounce_monthly as (
  select
    annee,
    mois,
    round(
      safe_divide(countif(nb_events = 1), count(*)) * 100,
      2
    ) as bounce_rate_pct
  from (
    select
      extract(year  from created_at) as annee,
      extract(month from created_at) as mois,
      session_id,
      count(*) as nb_events
    from `bigquery-public-data.thelook_ecommerce.events`
    where date(created_at) between date_debut and date_fin
    group by annee, mois, session_id
  )
  group by annee, mois
),

-- Logistique
lead_time_monthly as (
  select
    extract(year  from created_at) as annee,
    extract(month from created_at) as mois,
    round(
      avg(date_diff(shipped_at, created_at, day)),
      2
    ) as avg_lead_time_days
  from `bigquery-public-data.thelook_ecommerce.orders`
  where status in ('Shipped', 'Complete')
    and date(created_at) between date_debut and date_fin
  group by annee, mois
),

-- Rétention
retention_monthly as (
  select
    annee,
    mois,
    round(
      safe_divide(countif(total_orders > 1), count(*)) * 100,
      2
    ) as retention_rate_pct
  from (
    select
      extract(year  from created_at) as annee,
      extract(month from created_at) as mois,
      user_id,
      count(order_id) as total_orders
    from `bigquery-public-data.thelook_ecommerce.orders`
    where date(created_at) between date_debut and date_fin
    group by annee, mois, user_id
  )
  group by annee, mois
)

-- Final
select
  f.annee,
  f.category,
  f.mois,
  f.country,

  f.nb_commandes,
  f.nb_articles_vendus,
  f.chiffre_affaires,
  f.cout_total,
  f.marge,
  f.aov,
  f.taux_marge_brute_pct,
  f.taux_marge_nette_estimee_pct,
  f.return_rate_pct,

  c.conversion_rate_pct,
  b.bounce_rate_pct,
  l.avg_lead_time_days,
  r.retention_rate_pct

from financial_monthly f
left join conversion_monthly c
  on f.annee = c.annee and f.mois = c.mois
left join bounce_monthly b
  on f.annee = b.annee and f.mois = b.mois
left join lead_time_monthly l
  on f.annee = l.annee and f.mois = l.mois
left join retention_monthly r
  on f.annee = r.annee and f.mois = r.mois

order by f.annee, f.mois, f.country;