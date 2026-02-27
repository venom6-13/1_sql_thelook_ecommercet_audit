 select
  format('%d-%02d',
    extract(year  from o.delivered_at),
    extract(month from o.delivered_at))                           as annee_mois,

  count(distinct o.order_id)                                      as nb_commandes,
  round(sum(oi.sale_price), 2)                                    as chiffre_affaires,

  round(safe_divide(
    countif(oi.status = 'Returned'),
    count(*)) * 100, 2)                                           as return_rate_pct,

  -- Part de la Chine dans le CA total du mois
  round(safe_divide(
    sum(oi.sale_price),
    sum(sum(oi.sale_price)) over (
      partition by extract(year from o.delivered_at),
                   extract(month from o.delivered_at)
    )) * 100, 2)                                                  as part_ca_chine_pct

from `bigquery-public-data.thelook_ecommerce.order_items` oi
join `bigquery-public-data.thelook_ecommerce.orders`   o on o.order_id = oi.order_id
join `bigquery-public-data.thelook_ecommerce.products` p on p.id       = oi.product_id
join `bigquery-public-data.thelook_ecommerce.users`    u on u.id       = o.user_id
where p.category = 'Fashion Hoodies & Sweatshirts'
  and oi.status in ('Complete', 'Returned')
  and date(o.delivered_at) between '2025-01-01' and current_date()
  and u.country = 'China'
group by 1, o.delivered_at
order by 1;