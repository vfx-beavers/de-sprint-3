-- add column "status" в таблице staging.user_order_log
alter table
    staging.user_order_log
add column if not exists
    "status" varchar(10);

--add column "status" в таблице mart.f_sales
alter table
    de.mart.f_sales
add column if not exists
    "status" varchar(10);
