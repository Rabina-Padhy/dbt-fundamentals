with 

-- Import CTEs 
orders as (
  select * from {{ ref('stg_jaffle_shop__orders') }}
),

customers as (
  select * from {{ ref('stg_jaffle_shop__customers') }}
),
  
payments as (
  select * from {{ ref('stg_stripe__payments') }}
   where payments.payment_status != 'fail'
),

order_total as (

    select

      order_id,
      payment_status,
      sum(payment_amount) as order_value_dollars

    from payments
    group by 1, 2
),

order_values_joined as (

  select 
    orders.*,
    order_total.payment_status,
    order_total.order_value_dollars

  from orders
  left join order_total 
    on orders.order_id = order_total.order_id
)


-- marts
customer_order_history as (

    select 

        customers.customer_id,
        customers.full_name,
        customers.surname,
        customers.givenname,

        min(orders.order_date) as first_order_date,

        min(orders.valid_order_date) as first_non_returned_order_date,

        max(orders.valid_order_date) as most_recent_non_returned_order_date,

        coalesce(max(user_order_seq),0) as order_count,

        coalesce(
            count(case 
                  when orders.valid_order_date is not null 
                  then 1 
                  end)
            ,0) as non_returned_order_count,

        sum(case 
        when orders.valid_order_date is not null  
        then orders.order_value_dollars else 0 
        end) as total_lifetime_value,

        sum(case 
        when orders.valid_order_date is not null  
        then orders.order_value_dollars else 0 
        end)
        /nullif(count(case 
        when orders.valid_order_date is not null 
        then 1 
        end),0) as avg_non_returned_order_value,
        
        array_agg(distinct orders.order_id) as order_ids

    from orders

    join customers
    on orders.customer_id = customers.customer_id

    left outer join payments
    on orders.order_id = payments.order_id

    group by customers.customer_id, customers.full_name, customers.surname, customers.givenname

) ,

-- Final CTE
final as (
    select 

        orders.order_id,
        orders.customer_id,
        customers.surname,
        customers.givenname,
        first_order_date,
        order_count,
        total_lifetime_value,
        orders.order_value_dollars,
        orders.order_status,
        orders.payment_status

    from orders 

    join customers
    on orders.customer_id = customers.customer_id

    join customer_order_history
    on orders.customer_id = customer_order_history.customer_id 
    
)
-- Simple select statements
select * from final
