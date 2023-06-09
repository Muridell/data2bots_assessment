/*2. Total number of late shipments
 A late shipment is one with shipment_date greater than or equal to 6 days after the order_date 
 and delivery_date is NULL
 */

WITH ORDER_SHIPMENT AS(
 SELECT O.ORDER_ID, SHIPMENT_ID, ORDER_DATE, 
        SHIPMENT_DATE, DELIVERY_DATE,
        DATEDIFF(DAY, ORDER_DATE, SHIPMENT_DATE) DAYS_TO_SHIP
 FROM {{source('s3', 'ORDERS')}} O
 JOIN {{source('s3', 'SHIPMENT_DELIVERIES')}} SD
 ON O.ORDER_ID = SD.ORDER_ID
)

SELECT COUNT(*) LATE_SHIPMENTS FROM ORDER_SHIPMENT
WHERE DAYS_TO_SHIP >= 6 AND DELIVERY_DATE IS NULL