with cleaned as (
    select * from {{ ref('int_orders_cleaned') }}
),
customers as (
    select
        customer_id,
        customer_name,
        segment,
        count(distinct order_id)          as total_orders,
        round(sum(sales), 2)              as lifetime_sales,
        round(sum(profit), 2)             as lifetime_profit,
        round(avg(profit_margin_pct), 2)  as avg_profit_margin,
        sum(case when is_loss_making then 1 else 0 end)
                                          as loss_making_orders,
                                          min(order_date)                   as first_order_date,
        max(order_date)                   as last_order_date,
        date_diff(
            max(order_date),
            min(order_date),
            day
        )                                 as customer_lifespan_days

    from cleaned
    group by customer_id, customer_name, segment
)

select * from customers