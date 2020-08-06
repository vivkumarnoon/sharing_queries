with base as
(
/*
select order_date, order_month,cancellation_owner,is_shipped, is_delivered, is_cir,country_code,is_ndr,year,is_fbn,payment_method,repeat_customer_type, partner_country, */

 

 

 

--count(id_sales_order_item) as items,
select order_month,
COUNT(distinct IF((id_sales_order_item_status IN (2,3,4, 5, 6, 8) OR cancel_reason_code IN ("C4","C18","C30", "C31")), item_nr, null)) as total_items,
COUNT(distinct IF(id_sales_order_item_status IN (5, 6, 8), item_nr, null)) as fulfilled_items,
COUNT(distinct IF(ship_ff_within_tat=1 and id_sales_order_item_status IN (5, 6, 8), item_nr, null)) as within_tat_ff_items,

 

 

 

--sum(if(est_shipped_at>=shipped_at,1,0)) as wt_items,

 

 

 

from
(
select soi.*, 
CASE WHEN sti.shipped_at is not null and  DATE(sois.occurred_at) <= DATE(sti.shipped_at) THEN 1 ELSE 0 end as ship_ff_within_tat, 
so.country_code,Extract(Year from timestamp_add(so.created_at,interval(if(so.country_code='AE',4,if(so.country_code='SA',3,2))) hour)) as year,case when so.payment_method_code in ("cod") then "cod" else "prepaid" end as payment_method,
case when fpo.order_nr is null then "repeat" else "new" end as repeat_customer_type,
case
when soi.id_sales_order_item_status in (7) and soi.cancel_reason_code in ("C64") then "Reseller"
when soi.id_sales_order_item_status in (7) and soie.occurred_at is null and soi.cancel_reason_code in ("C14") then "cancelled_at_confirmation"
when soi.id_sales_order_item_status in (7) and soie.occurred_at is null then "Customer"
when soi.id_sales_order_item_status in (7) and soie.occurred_at is not null and soi.cancel_reason_code in ("C4","C18","C30") then if(case when pwh.is_fbn is null then poi.is_fbn else pwh.is_fbn end = 1,"Instock OOS", "Seller OOS")
when soi.id_sales_order_item_status in (7) and soie.occurred_at is not null and co.owner in ("CX") then "Customer"
when soi.id_sales_order_item_status in (7) and soie.occurred_at is not null and co.owner in ("Noon") then "Noon"
when soi.id_sales_order_item_status in (7) and soie.occurred_at is not null and co.owner in ("Noon/ Seller") then if(case when pwh.is_fbn is null then poi.is_fbn else pwh.is_fbn end = 1,"Noon", "Seller OOS")
when soi.id_sales_order_item_status in (7) and soie.occurred_at is not null and co.owner is not null then co.owner
when soi.id_sales_order_item_status in (7) then "check"
else null end as cancellation_owner,
case when soi.id_sales_order_item_status in (5,6,8) then 1 else 0 end as is_shipped,
case when soi.id_sales_order_item_status in (6) then 1 else 0 end as is_delivered,
case when soi.id_sales_order_item_status in (8) then 1 else 0 end as  is_ndr,
case when so.country_code = "SA" then (soi.paid_price+soi.wallet_money_value)*(.98)
when so.country_code="AE" then (soi.paid_price+soi.wallet_money_value)
else (soi.paid_price+soi.wallet_money_value)*.21 end as gmv_aed,
so.country_code as country,
Extract(Month from timestamp_add(soi.created_at,interval(if(so.country_code='AE',4,if(so.country_code='SA',3,2))) hour)) as order_month,
case when srri.id_sales_order_item is not null and id_sales_order_item_status=6 then 1 else 0 end as is_cir,
'' as order_date,
row_number() over (Partition by so.id_sales_order) as order_count,
-- case when off.id_partner in (1,9200,9800) then -1
-- when pwh.is_fbn is null then poi.is_fbn else pwh.is_fbn end as is_fbn,
poi.is_fbn,
sti.allocated_at as exp_allocated_at,
TIMESTAMP_ADD(if(wmsl.created_at is null,sbi.created_at,wmsl.created_at),INTERVAL if(so.country_code="AE",4,if(so.country_code='SA',3,2)) Hour) allocated_at,
cs.partner_country, sti.shipped_at est_shipped_at,sois.occurred_at shipped_at,
row_number() over (Partition by soi.id_sales_order_item) as unique,
from noondwh.sales.sales_order_item soi
left join noondwh.sales.sales_order so
using (id_sales_order)
left join (select id_sales_order_item,min(occurred_at) as occurred_at from noonbicenopa.ops.sales_order_item_status_history_pdate where created_at is not null and id_sales_order_item_status in (4) group by 1) soie
on soie.id_sales_order_item=soi.id_sales_order_item
left join (select id_sales_order_item,min(occurred_at) as occurred_at from noonbicenopa.ops.sales_order_item_status_history_pdate where created_at is not null and id_sales_order_item_status in (5) group by 1) soihs
on soihs.id_sales_order_item=soi.id_sales_order_item
left join (select id_sales_order_item,min(occurred_at) as occurred_at from noonbicenopa.ops.sales_order_item_status_history_pdate where created_at is not null and id_sales_order_item_status in (6) group by 1) soid
on soid.id_sales_order_item=soi.id_sales_order_item
left join noonbicenopa.ansharma.cancellation_owner co
on co.cancel_reason_code=soi.cancel_reason_code
LEFT JOIN `noonbimkpops.purchase.poi_min` poi
on poi.id_sales_order_item=soi.id_sales_order_item
left join noondwh.partner.partner_warehouse pwh
on pwh.id_partner_warehouse=soi.id_partner_warehouse
left join `noondwh.sales_return.sales_return_receipt_item` srri
on srri.id_sales_order_item=soi.id_sales_order_item
left join noonbicenopa.ansharma.first_placed_order fpo
on fpo.order_nr=so.order_nr
left join noondwh.offer.offer off
on soi.id_offer=off.id_offer
left join noondwh.oms.purchase_item pi
on pi.purchase_item_nr=poi.purchase_item_nr

 

 

 

left join noondwh.oms.stock_item sti
on sti.id_purchase_item=pi.id_purchase_item

 

 

 


left join (select id_purchase_item, max(created_at) as created_at from `noondwh.oms.stock_allocation_log` where id_log_type = 1 group by id_purchase_item) wmsl
on wmsl.id_purchase_item=pi.id_purchase_item
left join (select id_sales_order_item, allocated_at as created_at from noonbicenopa.ops.allocation)sbi
on soi.id_sales_order_item=sbi.id_sales_order_item
left join (select * from (select *,row_number() over (Partition by id_sales_order_item order by created_at desc) as col from noondwh.purchase_v2.purchase_order_item) where col=1) fpoi on fpoi.id_sales_order_item=soi.id_sales_order_item
left join noondwh.oms.purchase_item fpi on fpi.purchase_item_nr=fpoi.purchase_item_nr
left join noondwh.oms.stock_item fsti on fsti.id_purchase_item=fpi.id_purchase_item
left join noonbimkpops.g_sheets.china_sellers cs on cs.id_partner=fpoi.id_partner

 

 

 

left join (select id_sales_order_item,occurred_at from noonbicenopa.ops.sales_order_item_status_history_pdate where id_sales_order_item_status = 5) sois
on sois.id_sales_order_item = soi.id_sales_order_item

 

 

 


-- left join `noonbicenopa.ops.allocation` al
-- on al.id_sales_order_item = soi.id_sales_order_item
where soi.id_sales_order_item_status not in (1,9)
and soi.id_invoice_section=1
and Extract(Date from (timestamp_add(so.created_at,interval(if(so.country_code='AE',4,if(so.country_code='SA',3,2))) hour)))>="2019-10-01"
--and (soi.cancel_reason_code not in ("C21",'C64',"C67") or soi.cancel_reason_code is null)
AND (soi.CANCEL_REASON_CODE IN ('C18','C4','C30','C31') OR soi.CANCEL_REASON_CODE IS NULL)
and so.id_mp in (1)
and poi.is_fbn=0
and so.country_code="AE"
--and poi.id_partner not in (select id_partner from `noonbimkpops.g_sheets.china_sellers` )
and extract(year from soi.created_at)=2020
and Extract(Month from timestamp_add(soi.created_at,interval(if(so.country_code='AE',4,if(so.country_code='SA',3,2))) hour)) IN (8)
and date(sti.shipped_at)<current_date  --interval if(so.country_code='AE',4,(if(so.country_code='SA',3,2))) HOUR)  
)
where unique=1
#or order_month = 12) extract(month from date_add(current_date(),interval -1 month))
group by 1 --,2,3,4,5,6,7,8,9,10,11,12,13
)

 

 

 

 

 

SELECT  *, (within_tat_ff_items/Total_items) perc_wt_ff, 
(fulfilled_items/Total_items) perc_fulfilled, 

 

 

 

--(oos_items/Total_items) perc_oos, (pending/Total_items) perc_pending  
FROM BASE
