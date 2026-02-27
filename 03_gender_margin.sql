with monthly_by_gender as (
  select
    u.gender,
    sum(oi.sale_price)                                      as ca,
    round(safe_divide(
      sum(oi.sale_price - p.cost),
      sum(oi.sale_price)) * 100, 2)                         as taux_marge_brute_pct,
    count(distinct o.order_id)                              as nb_commandes
  from `bigquery-public-data.thelook_ecommerce.order_items` oi
  join `bigquery-public-data.thelook_ecommerce.orders`   o on o.order_id = oi.order_id
  join `bigquery-public-data.thelook_ecommerce.products` p on p.id       = oi.product_id
  join `bigquery-public-data.thelook_ecommerce.users`    u on u.id       = o.user_id
  where p.category = 'Fashion Hoodies & Sweatshirts'
    and oi.status in ('Complete', 'Returned')
    and date(o.delivered_at) between '2025-01-01' and current_date()
  group by 1
),

total as (
  select sum(ca) as ca_total from monthly_by_gender
)

select
  g.gender,
  round(g.ca, 2)                                           as chiffre_affaires,
  round(safe_divide(g.ca, t.ca_total) * 100, 2)           as part_ca_pct,
  g.taux_marge_brute_pct,
  g.nb_commandes
from monthly_by_gender g
cross join total t
order by g.ca desc;