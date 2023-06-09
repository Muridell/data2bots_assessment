SELECT O.ORDER_ID,
        O.CUSTOMER_ID,
        CUSTOMER_NAME,
        C.POSTAL_CODE,
        A.COUNTRY,
        A.REGION,
        A.STATE,
        A.ADDRESS,
        O.ORDER_DATE,
        DD.WORKING_DAY,
        CASE WHEN (DAY_OF_THE_WEEK_NUM BETWEEN 1 AND 5) AND WORKING_DAY = 'False' THEN 'Public Holiday'
             WHEN (DAY_OF_THE_WEEK_NUM IN (6, 7)) THEN 'Weekend'
             ELSE 'Work Day'
        END DAY_TYPE,
        DD.DAY_OF_THE_WEEK_NUM DAY_OF_THE_WEEK,
        DAYNAME(ORDER_DATE) DAY,
        DD.MONTH_OF_THE_YEAR_NUM MONTH_OF_THE_YEAR,
        MONTHNAME(ORDER_DATE) MONTH,
        O.PRODUCT_ID,
        PRODUCT_NAME,
        PRODUCT_CATEGORY,
        UNIT_PRICE,
        QUANTITY,
        AMOUNT,
        SHIPMENT_ID,
        SHIPMENT_DATE,
        DAYNAME(SHIPMENT_DATE) SHIPMENT_DAY,
        DELIVERY_DATE,
        DAYNAME(DELIVERY_DATE) DELIVERY_DAY,
        DATEDIFF(DAY, ORDER_DATE, SHIPMENT_DATE) DAYS_TO_SHIP,
        CASE WHEN (DATEDIFF(DAY, ORDER_DATE, SHIPMENT_DATE) >= 6 AND DELIVERY_DATE IS NULL) THEN 'Late'
            WHEN (SHIPMENT_DATE IS NULL AND DELIVERY_DATE IS NULL 
                    AND DATEDIFF(DAY, ORDER_DATE, '2022-09-05') >= 15) THEN 'Undelivered'
            ELSE 'Early'
        END SHIPMENT_TYPE
FROM {{source('s3', 'ORDERS')}} O
JOIN {{source('s3', 'SHIPMENT_DELIVERIES')}} SD
ON O.ORDER_ID = SD.ORDER_ID
JOIN {{source('rds', 'DIM_CUSTOMERS')}} C
ON C.CUSTOMER_ID = O.CUSTOMER_ID
JOIN {{source('rds', 'DIM_ADDRESSES')}} A
ON C.POSTAL_CODE = A.POSTAL_CODE
JOIN {{source('rds', 'DIM_DATES')}} DD
ON (O.ORDER_DATE = DD.CALENDAR_DT)
JOIN {{source('rds', 'DIM_PRODUCTS')}} P
ON O.PRODUCT_ID = P.PRODUCT_ID