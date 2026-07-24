with raw as (
    select * from {{ ref('stg_orders') }}
),

cleaned as (
    select

        cast(row_id as int64)                       as row_id,
        cast(order_id as string)                    as order_id,
        cast(parse_date('%m/%d/%Y', order_date) as date)  as order_date,
        cast(parse_date('%m/%d/%Y', ship_date)  as date)  as ship_date,
        initcap(trim(ship_mode))                    as ship_mode,
        cast(customer_id as string)                 as customer_id,
        initcap(trim(customer_name))                as customer_name,
        initcap(trim(segment))                      as segment,
        initcap(trim(city))                         as city,
        initcap(trim(state))                        as state,
        upper(trim(region))                         as region,
        upper(trim(country))                        as country,
        cast(postal_code as string)                 as postal_code,
        cast(product_id as string)                  as product_id,
        trim(product_name)                          as product_name,
        initcap(trim(category))                     as category,
        initcap(trim(sub_category))                 as sub_category,
        cast(quantity as int64)                     as quantity,
        round(cast(sales as float64), 2)            as sales,
        round(cast(discount as float64), 4)         as discount,
        round(cast(profit as float64), 2)           as profit,
        date_diff(
            cast(parse_date('%m/%d/%Y', ship_date) as date),
            cast(parse_date('%m/%d/%Y', order_date) as date),
            day
        )                                           as days_to_ship,
        round(
            cast(profit as float64)
            / nullif(cast(sales as float64), 0) * 100,
            2
        )                                           as profit_margin_pct,
        round(
            cast(sales as float64) * (1 - cast(discount as float64)),
            2
        )                                           as discounted_sales,
        case
            when cast(profit as float64) < 0 then true
            else false
        end                                         as is_loss_making,
        case
            when date_diff(
                cast(parse_date('%m/%d/%Y', ship_date) as date),
                cast(parse_date('%m/%d/%Y', order_date) as date),
                day
            ) = 0 then 'Same Day'
            when date_diff(
                cast(parse_date('%m/%d/%Y', ship_date) as date),
                cast(parse_date('%m/%d/%Y', order_date) as date),
                day
            ) <= 2 then 'Fast'
            when date_diff(
                cast(parse_date('%m/%d/%Y', ship_date) as date),
                cast(parse_date('%m/%d/%Y', order_date) as date),
                day
            ) <= 5 then 'Standard'
            else 'Slow'
        end                                         as shipping_speed,
        extract(year from cast(parse_date('%m/%d/%Y', order_date) as date))
                                                    as order_year,
        extract(month from cast(parse_date('%m/%d/%Y', order_date) as date))
                                                    as order_month,
        format_date(
            '%Y-%m',
            cast(parse_date('%m/%d/%Y', order_date) as date)
        )                                           as order_year_month,
        current_timestamp()                         as loaded_at

    from raw

    where order_id is not null
)

select * from cleaned