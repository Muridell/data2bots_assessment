/*3. Total number of undelivered shipments 
An undelivered shipment is one with delivery_date as NULL and shipment_date as NULL 
and the current_date 15 days after order_date.

current_date here refers to 2022-09-05 which is the max(order_date)
*/

WITH ORDER_SHIPMENT AS(
 SELECT O.ORDER_ID, SHIPMENT_ID, ORDER_DATE, 
        SHIPMENT_DATE, DELIVERY_DATE,
        '2022-09-05'::DATE AS CURRENT_DATE,
        DATEDIFF(DAY, ORDER_DATE, '2022-09-05') DAYS_TO_SHIP
 FROM {{source('s3', 'ORDERS')}} O
 JOIN {{source('s3', 'SHIPMENT_DELIVERIES')}} SD
 ON O.ORDER_ID = SD.ORDER_ID
)

SELECT COUNT(*) UNDELIVERED_SHIPMENTS FROM ORDER_SHIPMENT
WHERE SHIPMENT_DATE IS NULL AND DELIVERY_DATE IS NULL AND DAYS_TO_SHIP >= 15