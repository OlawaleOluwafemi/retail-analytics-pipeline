
  
    
    
    
        
         


        
  

  insert into `retail_analyticsdb`.`mart_location_performance`
        ("store_location", "product_category", "total_orders", "total_items_sold", "total_revenue", "average_order_value")

SELECT
    store_location,
    product_category,
    COUNT(transaction_id) as total_orders,
    SUM(item_count) as total_items_sold,
    SUM(purchase_amount) as total_revenue,
    ROUND(AVG(purchase_amount), 2) as average_order_value
FROM `retail_analyticsdb`.`stg_transactions`
GROUP BY 
    store_location, 
    product_category
  