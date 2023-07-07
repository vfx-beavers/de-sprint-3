drop table if exists mart.f_customer_retention;
                create table mart.f_customer_retention
                as 
                (with cus as (select date_part('week',date_time) weekly
                                , date_part('month',date_time) monthly
                                , customer_id 
                                , quantity 
                                , payment_amount 
                                , case when count(quantity) over(partition by  customer_id ,date_part('week',date_time)) = 1 then 'new' else status end status
                                from staging.user_order_log uol )
 
                select count(distinct case when status = 'new' then customer_id end) new_customers_count
                ,count(distinct case when status != 'new' then customer_id end) returning_customers_count 
                ,count(distinct case when status = 'refunded' then customer_id end) refunded_customer_count
                ,weekly
                ,monthly
                ,sum(case when status = 'new' then payment_amount end) new_customers_revenue 
                ,sum(case when status != 'new' then payment_amount end) returning_customers_revenue  
                ,sum(case when status = 'refunded' then quantity end) customers_refunded   
                from cus
                group by weekly,monthly);