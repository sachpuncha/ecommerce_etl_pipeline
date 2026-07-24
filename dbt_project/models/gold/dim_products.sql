with cleaned as (
    select * from {{ ref('int_orders_cleaned') }}
),

products as (
    select
        product_id,
        product_name,
        category,
        sub_category,
        count(distinct order_id)          as times_ordered,
        round(avg(sales), 2)              as avg_selling_price,
        round(avg(discount), 2)           as avg_discount_given,
        round(avg(profit_margin_pct), 2)  as avg_profit_margin,
        round(sum(profit), 2)             as total_profit_generated,
        sum(case when is_loss_making then 1 else 0 end)
                                          as times_sold_at_loss

    from cleaned
    group by product_id, product_name, category, sub_category
)

select * from products