with cleaned as (
    select * from {{ ref('int_orders_cleaned') }}
),

geography as (
    select
        city,
        state,
        region,
        country,
        postal_code,
        count(distinct order_id)          as total_orders,
        count(distinct customer_id)       as unique_customers,
        round(sum(sales), 2)              as total_sales,
        round(sum(profit), 2)             as total_profit,
        round(avg(profit_margin_pct), 2)  as avg_profit_margin

    from cleaned
    group by city, state, region, country, postal_code
)

select * from geography