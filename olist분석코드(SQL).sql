-- 요일컬럼 추가
ALTER TABLE olist.total
ADD COLUMN day_of_week TEXT;

update olist.total
set day_of_week = TO_CHAR(order_purchase_timestamp::timestamp, 'Day');

-- 시간컬럼 추가
ALTER TABLE olist.total
ADD COLUMN hour TEXT;

update olist.total
set hour = TO_CHAR(order_purchase_timestamp::timestamp, 'HH24');

-- 주이름 풀네임으로 조인하기 위한 테이블 생성 
CREATE TABLE brazil_states (
    state_code VARCHAR(2), 
    state_name VARCHAR(50)
);

INSERT INTO brazil_states (state_code, state_name)
VALUES 
    ('AC', 'Acre'),
    ('AL', 'Alagoas'),
    ('AM', 'Amazonas'),
    ('AP', 'Amapá'),
    ('BA', 'Bahia'),
    ('CE', 'Ceará'),
    ('DF', 'Distrito Federal'),
    ('ES', 'Espírito Santo'),
    ('GO', 'Goiás'),
    ('MA', 'Maranhão'),
    ('MG', 'Minas Gerais'),
    ('MS', 'Mato Grosso do Sul'),
    ('MT', 'Mato Grosso'),
    ('PA', 'Pará'),
    ('PB', 'Paraíba'),
    ('PE', 'Pernambuco'),
    ('PI', 'Piauí'),
    ('PR', 'Paraná'),
    ('RJ', 'Rio de Janeiro'),
    ('RN', 'Rio Grande do Norte'),
    ('RO', 'Rondônia'),
    ('RR', 'Roraima'),
    ('RS', 'Rio Grande do Sul'),
    ('SC', 'Santa Catarina'),
    ('SE', 'Sergipe'),
    ('SP', 'São Paulo'),
    ('TO', 'Tocantins');


--  
create table olist.sales_m as(
select to_char(order_purchase_timestamp::timestamp,'yyyy-mm') date_or,round(sum(payment_value)) total_sales from olist.total
where is_revenue='y'
group by 1);

select to_char(order_purchase_timestamp::timestamp,'yyyy-mm') date_or,round(sum(payment_value)) total_sales from olist.total
where is_revenue='y'
group by 1


select max(order_purchase_timestamp) from olist.total
where to_char(order_purchase_timestamp::date,'yyyy-mm')='2018-08' and is_revenue='y';

-- 2017/09~2018/08 12개월 기간의 데이터로 rfm적용하여 계산
create table olist.RFM as(
select customer_unique_id,max(order_purchase_timestamp::date) max_date,
(select max(order_purchase_timestamp) from olist.total
where to_char(order_purchase_timestamp::date,'yyyy-mm')='2018-08')::date base_date,
((select max(order_purchase_timestamp) from olist.total
where to_char(order_purchase_timestamp::date,'yyyy-mm')='2018-08')::date-max(order_purchase_timestamp::date)) recency,
count(*) frequency,
sum(payment_value) monetary
from olist.total 
where is_revenue='y' and date_trunc('month', order_purchase_timestamp::date) >= '2017-09-01'::date and 
date_trunc('month', order_purchase_timestamp::date) < '2018-09-01'::date
group by 1);


--
create table cohort_table as(
with cohort_data as(
select customer_unique_id,
min(date_trunc('month',order_purchase_timestamp::date)) cohort_m
from olist.total
where is_revenue='y' and date_trunc('month', order_purchase_timestamp::date) >= '2017-09-01'::date and 
date_trunc('month', order_purchase_timestamp::date) < '2018-09-01'::date
group by 1),
act_data as(
select c.cohort_m,
date_trunc('month',t.order_purchase_timestamp::date) act_m,
count(distinct t.customer_unique_id) act_users
from cohort_data c
join (select * from olist.total
where is_revenue='y' and date_trunc('month', order_purchase_timestamp::date) >= '2017-09-01'::date and 
date_trunc('month', order_purchase_timestamp::date) < '2018-09-01'::date) t
on c.customer_unique_id=t.customer_unique_id
group by 1,2)
select 
to_char(cohort_m,'yyyy-mm') cohort_mm,
to_char(act_m,'yyyy-mm') act_mm,
extract('month'from age(act_m,cohort_m)) month_diff,
act_users
from act_data
order by 1,3);

select distinct customer_unique_id from olist.total
where is_revenue='y' and date_trunc('month', order_purchase_timestamp::date) >= '2017-09-01'::date and 
date_trunc('month', order_purchase_timestamp::date) < '2018-09-01'::date and 
date_trunc('month', order_purchase_timestamp::date)='2017-10-01';


with cohort_data as(
select customer_unique_id,
min(date_trunc('month',order_purchase_timestamp::date)) cohort_m
from olist.total
where is_revenue='y' and date_trunc('month', order_purchase_timestamp::date) >= '2017-09-01'::date and 
date_trunc('month', order_purchase_timestamp::date) < '2018-09-01'::date
group by 1;

select customer_unique_id,product_category_name_english,
count(customer_unique_id) cnt from olist.total
where is_revenue='y' and 
date_trunc('month', order_purchase_timestamp::date) >= '2017-09-01'::date and 
date_trunc('month', order_purchase_timestamp::date) < '2018-09-01'::date 
group by customer_unique_id,product_category_name_english;

select * from olist.rfm;

create table rfm_cate as(
select a.*,product_category_name_english,cnt,sales from olist.rfm a left join 
(select customer_unique_id,product_category_name_english,
count(customer_unique_id) cnt,sum(payment_value) sales from olist.total
where is_revenue='y' and 
date_trunc('month', order_purchase_timestamp::date) >= '2017-09-01'::date and 
date_trunc('month', order_purchase_timestamp::date) < '2018-09-01'::date 
group by customer_unique_id,product_category_name_english) b
on a.customer_unique_id=b.customer_unique_id);