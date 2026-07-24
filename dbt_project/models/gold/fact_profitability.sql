with sales as (
    select * from {{ ref('fct_sales') }}
),
profitability as (
    select
        order_year,
        order_year_month,
        region,
        category,
        sub_category,
        segment,
        shipping_speed,
        count(distinct order_id)          as total_orders,
        sum(quantity)                     as total_units_sold,
        round(sum(sales), 2)              as total_revenue,
        round(sum(discounted_sales), 2)   as total_discounted_revenue,
        round(sum(profit), 2)             as total_profit,
        round(avg(profit_margin_pct), 2)  as avg_profit_margin,
        round(sum(discount), 2)           as total_discount_given,
        round(avg(days_to_ship), 1)       as avg_days_to_ship,
        sum(case when is_loss_making
            then 1 else 0 end)            as loss_making_orders,
            round(
            sum(sum(sales)) over (
                partition by region
                order by order_year_month
                rows between unbounded preceding and current row
            ), 2
        )                                 as running_revenue_by_region

    from sales
    group by
        order_year,
        order_year_month,
        region,
        category,
        sub_category,
        segment,
        shipping_speed
)

select * from profitability