select
  format('%d-%02d',
    extract(year  from o.delivered_at),
    extract(month from o.delivered_at))                           as annee_mois,

  round(safe_divide(
    countif(oi.status = 'Returned'),
    count(*)) * 100, 2)                                           as return_rate_pct,

  round(safe_divide(
    sum(oi.sale_price - p.cost),
    sum(oi.sale_price)) * 100, 2)                                 as taux_marge_brute_pct,

  round(safe_divide(
    sum(oi.sale_price - p.cost)
      - countif(oi.status = 'Returned') * avg(oi.sale_price) * 0.15,
    sum(oi.sale_price)) * 100, 2)                                 as taux_marge_nette_pct

from `bigquery-public-data.thelook_ecommerce.order_items` oi
join `bigquery-public-data.thelook_ecommerce.orders`   o on o.order_id = oi.order_id
join `bigquery-public-data.thelook_ecommerce.products` p on p.id       = oi.product_id
where p.category = 'Fashion Hoodies & Sweatshirts'
  and oi.status in ('Complete', 'Returned')
  and date(o.delivered_at) between '2025-01-01' and current_date()
group by 1
order by 1;