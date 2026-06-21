

SELECT
    store_location,
    product_category,
    COUNT(transaction_id) as total_orders,
    SUM(item_count) as total_items_sold,
    SUM(purchase_amount) as total_revenue,
    ROUND(AVG(purchase_amount), 2) as average_order_value,
    CAST(transaction_timestamp AS Date) AS transaction_date
FROM `retail_analyticsdb`.`stg_transactions`
GROUP BY 
    store_location, 
    product_category,
    transaction_timestamp