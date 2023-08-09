create or replace table discovery_sandbox.nma_sample_serach_data_v1 as (
with a as (select 
session_start_timestamp::date as session_date,
dataset,
row_type,
session_id ,
--authentication_state,
NEW_CUSTOMER_FLAG,
NEW_NMA_CUSTOMER_FLAG,
ACTIVE_AUTOSHIP_DERIVED,
CHANNEL_GROUPING,
CHANNEL_GROUPING_LAST_NON_DIRECT,
PERSONALIZATION_ID::varchar as personalization_id,
event_id,
customer_id,
event_date,
event_timestamp,
replace(properties:search_input_keyword, '"', '')::varchar as search_input_keyword,
replace(properties:search_output_keyword, '"', '')::varchar as search_output_keyword,
properties:nma_linker_id::varchar as nma_linker_id,
properties:number_of_suggestions as number_of_suggestions,
properties:search_experience_type::varchar as search_experience_type_event,


properties:list_category::varchar as list_category,
properties:list_id::varchar as list_id,
widget_id::varchar as widget_id,
AUTHENTICATION_STATE::varchar as AUTHENTICATION_STATE,
replace(event_label, '"', '')::varchar as event_label,
--event_action,
replace(event_category, '"', '')::varchar as event_category,
user_agent::varchar as user_agent,
 context:app:version::varchar as app_version,
replace(properties:event_action, '"', '')::varchar as event_action,
context:traits:page_type::varchar as page_type,
properties:facets_applied as facets_applied,
replace(event_name, '"', '')::varchar as event_name,
screen_name::varchar as screen_name,
is_bounce,
is_entrance,
is_exit,
--products,
product_id,
product_sku,
products[0]:product_deals as product_deals_flags,
case when properties:impression_id::varchar is not null then 1 else 0 end as sponsored_products_flag,
properties:impression_id::varchar as sponsored_products_piq_id,
case when row_type = 'product' then products[0]:position::numeric else null end as product_position,
--WIDGET_ID,
WIDGET_CREATIVE_NAME::varchar as WIDGET_CREATIVE_NAME,
--properties,
  last_value
    (
      case
      when replace(event_name, '"', '')::varchar = 'Products Searched'
      then EVENT_id
      end ignore nulls
    )
    over
    (
      partition by session_id, replace(properties:search_input_keyword, '"', '')::varchar
      order by EVENT_TIMESTAMP
      rows between unbounded preceding and current row
    ) 
    as search_id_1 ,
    
      first_value
    (
      case
      when replace(event_name, '"', '')::varchar = 'Products Searched'
      then EVENT_id
      end ignore nulls
    )
    over
    (
      partition by session_id, replace(properties:search_input_keyword, '"', '')::varchar
      order by EVENT_TIMESTAMP
      rows between current row and unbounded following
    ) 
    as search_id_2 ,
    
    nvl(search_id_1 ,search_id_2) as search_id,
    

    
  (
      case
      when replace(event_name, '"', '')::varchar = 'Products Searched'
      then 


lead
      (
        case 
        when replace(event_name, '"', '')::varchar = 'Products Searched'
        then replace(properties:search_input_keyword, '"', '')::varchar
        else null
        end
      )
      ignore nulls
      over (partition by  session_id order by EVENT_TIMESTAMP)
      else null
      end
    )
    as next_search_term,
    
    max(
    case when 
    replace(event_name, '"', '')::varchar = 'Product Added' 
    then 1 else 0 end)
   
    over(partition by session_id,product_sku)    
     as product_added,
    
       
        max(
    case when 
    replace(event_name, '"', '')::varchar = 'Checkout Started' 
    then 1 else 0 end)
    
    over(partition by session_id,product_sku)    
     as checkout_started,
     
             max(
    case when 
    event_name = 'Order Completed' 
    then 1 else 0 end)
   
    over(partition by session_id,product_sku)    
     as order_completed
   

 from  SEGMENT.SEGMENT_NMA_HITS_PRODUCTS_UNION where session_start_timestamp::date  =  '2023-08-07' )
 
 select *,       first_value
    (
      case
      when event_name::varchar = 'Screen Viewed'
      then search_experience_type_event
      end ignore nulls
    )
    over
    (
      partition by search_id
      order by EVENT_TIMESTAMP
         ) 
    as search_experience_type
    from a
 ) ; 
