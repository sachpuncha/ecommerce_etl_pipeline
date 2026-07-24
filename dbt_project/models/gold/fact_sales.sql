with cleaned as (
    select * from {{ ref('int_orders_cleaned') }}
),

customers as (
    select customer_id, total_orders as customer_total_orders
    from {{ ref('dim_customers') }}
),

products as (
    select product_id, avg_profit_margin as product_avg_margin
    from {{ ref('dim_products') }}
),

final as (
    select
        o.order_id,
        o.row_id,
        o.customer_id,
        o.product_id,
        o.order_date,
        o.ship_date,
        o.order_year,
        o.order_month,
        o.order_year_month,
        o.ship_mode,
        o.days_to_ship,
        o.shipping_speed,
        o.customer_name,
        o.segment,
        c.customer_total_orders,
        o.product_name,
        o.category,
        o.sub_category,
        o.city,
        o.state,
        o.region,
        o.country,
        o.postal_code,
        o.quantity,
        o.sales,
        o.discount,
        o.discounted_sales,
        o.profit,
        o.profit_margin_pct,
        o.is_loss_making

    from cleaned o
    left join customers c on o.customer_id = c.customer_id
    left join products p  on o.product_id  = p.product_id
)

select * from final