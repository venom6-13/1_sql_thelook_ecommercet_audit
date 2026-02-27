select
  extract(year  from o.delivered_at)                             as annee,
  extract(month from o.delivered_at)                             as mois,
  u.traffic_source,

  count(*)                                                        as nb_articles,

  round(safe_divide(
    countif(oi.status = 'Returned'),
    count(*)) * 100, 2)                                           as return_rate_pct

from `bigquery-public-data.thelook_ecommerce.order_items` oi
join `bigquery-public-data.thelook_ecommerce.orders`   o on o.order_id = oi.order_id
join `bigquery-public-data.thelook_ecommerce.products` p on p.id       = oi.product_id
join `bigquery-public-data.thelook_ecommerce.users`    u on u.id       = o.user_id
where p.category = 'Fashion Hoodies & Sweatshirts'
  and oi.status in ('Complete', 'Returned')
  and date(o.delivered_at) between '2025-01-01' and current_date()
  and extract(month from o.delivered_at) = 1  -- Focus janvier
group by 1, 2, 3
order by 1, return_rate_pct desc;