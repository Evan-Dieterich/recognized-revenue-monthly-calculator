-- ***** RECOGNIZED RECURRING REVENUE CALCULATOR *****

/* This script creates a working table that pulls monthly subscriptions from the payments table
	extracts their state and zip from a metadata column using json_extract. There are cases where
	state and zip metadata is null for several records - this is corrected below. Yearly subscriptions
	are recognized monthly despite being paid once per year, this is handled using a recursive cte below.
	Finally, the two result sets are combined and aggregated by zip and month for schools in NY only.
*/

-- create working table of monthly subscriptions
create table working_recognized_revenue as
select id, 
	customer_id,
	json_extract(payment_data, '$.source.address_state') as state,
	json_extract(payment_data, '$.source.address_zip') as zip, 
	payment_at,
	0 next_payment_at,
	amount,
	plan_id,
	0 prorated_amount,
	0 carryover_balance,
	0 total_amount,
	0 skipped_months,
	0 month
from payments
where plan_id in (
	select plan_id from school_plans
	where plan_interval = 'month')
order by customer_id;

-- create table for correct location data by school
create table correct_state_zip as
select * from working_recognized_revenue
where state is not null and zip is not null
group by customer_id;

-- update missing state in working table
update working_recognized_revenue
set state = (select state from correct_state_zip where customer_id = working_recognized_revenue.customer_id)
where state is null;

-- update missing zip in working table
update working_recognized_revenue
set zip = (select zip from correct_state_zip where customer_id = working_recognized_revenue.customer_id)
where zip is null;

-- delete all non NY states
delete from working_recognized_revenue
where state <> 'NY';

-- calculate pro-rated amount for current month, unless payment was made on the first
update working_recognized_revenue
set prorated_amount = 
	case 
		when strftime('%d', payment_at) = 1 then amount
		else(julianday(date(payment_at,'start of month','+1 month')) 
			- julianday(payment_at))
			/ strftime('%d', date(payment_at,'start of month','+1 month','-1 day'))
			* amount
	end
where plan_id in (
	select plan_id from school_plans
	where plan_interval = 'month');
	
-- use lead to align next payment_at in window with current month to compare dates
with future_payment as (
	select id, 
		customer_id,
		lead(payment_at) over(partition by customer_id order by customer_id, id) as next_payment
	from working_recognized_revenue)
update working_recognized_revenue
set next_payment_at = (select next_payment from future_payment where id = working_recognized_revenue.id);

-- insert new records below gaps in months and final payment months to allocate row for carryover
insert into working_recognized_revenue (id, customer_id, state, zip, payment_at, next_payment_at, amount, plan_id, prorated_amount, carryover_balance, total_amount, skipped_months, month)
	select id + 1,
	customer_id,
	state,
	zip, 
	DATE(payment_at ,'start of month', '+1 month'),
	null,
	amount,
	0, 
	0,
	0,
	0,
	0,
	0
	from working_recognized_revenue
	where (skipped_months <> 1 or skipped_months is NULL)
	and strftime('%m', payment_at) <> '12';
	
-- use lag to carry over remaining balance to next month for each customer
with carryover as (
	select id,
		customer_id,
		prorated_amount,
		lag(amount - prorated_amount) over(partition by customer_id order by customer_id, id) as prev_month_carryover,
		lag(prorated_amount) over(partition by customer_id order by customer_id, id) as prev_prorated_amount
	from working_recognized_revenue
)
update working_recognized_revenue
set carryover_balance = (select prev_month_carryover from carryover where id = working_recognized_revenue.id)
where id in (select id from carryover where prev_prorated_amount <> 0);

-- update null values to 0 for months with no carryover_balance
update working_recognized_revenue
set carryover_balance = 0
where carryover_balance is null;

-- set month
update working_recognized_revenue
set month = strftime('%m', payment_at)

-- calculate recognized revenue 
update working_recognized_revenue
set total_amount = round((prorated_amount + carryover_balance), 2)

-- *****ANNUAL SUBSCRIPTIONS *****

-- calculate revenue for yearly subscription by month using recursive cte
CREATE TEMPORARY TABLE YearlyRevenue as with cte as (
  select
	0 id,
    customer_id, 
    json_extract( payment_data, '$.source.address_state' ) as state, 
    json_extract( payment_data, '$.source.address_zip' ) as zip, 
    P.amount, 
    P.plan_id, 
    payment_at, 
    julianday( DATE(  payment_at, 'start of month', '+1 month',  '-1 day' ) ) - julianday(payment_at) as NoOfDays, 
    DATE( payment_at, 'start of month', '+1 month',  '-1 day' ) as lastMonthDay, 
    DATE( payment_at, 'start of year', '+12 month',   '-1 day' ) as lastYearDate, 
    DATE(payment_at, '+1 years') as LastSubscriptionDate, 
    S.plan_interval 
  from 
    payments P 
    join school_plans S on P.plan_id = S.plan_id 
  where 
    json_extract( payment_data, '$.source.address_state'  ) = 'NY' 
    AND S.plan_interval = 'year' 
  union all 
  select
	id + 1,
    customer_id, 
    state, 
    zip, 
    amount, 
    plan_id, 
    DATE(payment_at, '+30 days') as payment_at, 
    0 as NoOfDays, 
    lastMonthDay, 
    lastYearDate, 
    LastSubscriptionDate, 
    plan_interval 
  from cte 
  where 
    LastSubscriptionDate > DATE(payment_at, '+30 days')
) 
select *,
  strftime('%Y-%m', payment_at) as Month, 
  case when NoOfDays == 0 then amount / 12 else 
  (NoOfDays / strftime('%d', lastMonthDay) )*(amount / 12) end as currentMonthRevenue 
from 
  cte 
order by 
  customer_id asc;

-- verify results from recursive cte  
select * from YearlyRevenue;

-- carryover first month remaining balance to final month
with final_month as (
	select customer_id, id, amount, currentMonthRevenue,  first_value(amount / 12 - currentMonthRevenue)
	over (partition by customer_id order by customer_id, payment_at) as final_payment from YearlyRevenue)
update YearlyRevenue
set currentMonthRevenue = (select final_payment from final_month where customer_id = YearlyRevenue.customer_id)
where id = 12;

-- final report (monthly and annual recognized revenue grouped by month/zip in NY) for 2018
select month, state, zip, sum(total_amount) as monthly_total
from working_recognized_revenue
where state = 'NY'
union all
select substr(month, -2) as month, state, zip, sum(currentMonthRevenue) as monthly_total
from YearlyRevenue yr
where substr(yr.month, 4) <> '2019'
group by month, zip
order by month;


