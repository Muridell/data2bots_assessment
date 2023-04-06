/*
1. We want to obtain the following insights from the data:
The total number of orders placed on a public holiday every month for the past year'
A public holiday is a day with a day_of_the_week number in the range 1 - 5 and a working_day value of false. 
After your transformation, the derived table agg_public_holiday should be loaded 
into the {your_id}_analytics schema, using the below table
schema

All ingestion_date are the current date the table was generated
*/

WITH ORDER_CALENAR AS (
SELECT DATE(SYSDATE()) INGESTION_DATE,
        ORDER_ID,
        ORDER_DATE,
        CALENDAR_DT,
        MONTH_OF_THE_YEAR_NUM MONTH,
        DAY_OF_THE_MONTH_NUM DAY_MONTH,
        DAY_OF_THE_WEEK_NUM DAY_OF_THE_WEEK,
        WORKING_DAY,
        CASE WHEN (DAY_OF_THE_WEEK_NUM BETWEEN 1 AND 5) AND WORKING_DAY = 'False' THEN 'Public Holiday'
             WHEN (DAY_OF_THE_WEEK_NUM IN (6, 7)) THEN 'Weekend'
             ELSE 'Work day'
        END IS_PUBLIC_HOLIDAY
FROM {{source('s3', 'ORDERS')}} O
LEFT JOIN {{source('rds', 'DIM_DATES')}} DD
ON O.ORDER_DATE = DD.CALENDAR_DT
)

SELECT DISTINCT INGESTION_DATE,
(SELECT COUNT(*) FROM ORDER_CALENAR WHERE IS_PUBLIC_HOLIDAY = 'Public Holiday' AND MONTH = 1) TT_ORDER_HOL_JAN,
(SELECT COUNT(*) FROM ORDER_CALENAR WHERE IS_PUBLIC_HOLIDAY = 'Public Holiday' AND MONTH = 2) TT_ORDER_HOL_FEB,
(SELECT COUNT(*) FROM ORDER_CALENAR WHERE IS_PUBLIC_HOLIDAY = 'Public Holiday' AND MONTH = 3) TT_ORDER_HOL_MAR,
(SELECT COUNT(*) FROM ORDER_CALENAR WHERE IS_PUBLIC_HOLIDAY = 'Public Holiday' AND MONTH = 4) TT_ORDER_HOL_APR,
(SELECT COUNT(*) FROM ORDER_CALENAR WHERE IS_PUBLIC_HOLIDAY = 'Public Holiday' AND MONTH = 5) TT_ORDER_HOL_MAY,
(SELECT COUNT(*) FROM ORDER_CALENAR WHERE IS_PUBLIC_HOLIDAY = 'Public Holiday' AND MONTH = 6) TT_ORDER_HOL_JUN,
(SELECT COUNT(*) FROM ORDER_CALENAR WHERE IS_PUBLIC_HOLIDAY = 'Public Holiday' AND MONTH = 7) TT_ORDER_HOL_JUL,
(SELECT COUNT(*) FROM ORDER_CALENAR WHERE IS_PUBLIC_HOLIDAY = 'Public Holiday' AND MONTH = 8) TT_ORDER_HOL_AUG,
(SELECT COUNT(*) FROM ORDER_CALENAR WHERE IS_PUBLIC_HOLIDAY = 'Public Holiday' AND MONTH = 9) TT_ORDER_HOL_SEP,
(SELECT COUNT(*) FROM ORDER_CALENAR WHERE IS_PUBLIC_HOLIDAY = 'Public Holiday' AND MONTH = 10) TT_ORDER_HOL_OCT,
(SELECT COUNT(*) FROM ORDER_CALENAR WHERE IS_PUBLIC_HOLIDAY = 'Public Holiday' AND MONTH = 11) TT_ORDER_HOL_NOV,
(SELECT COUNT(*) FROM ORDER_CALENAR WHERE IS_PUBLIC_HOLIDAY = 'Public Holiday' AND MONTH = 12) TT_ORDER_HOL_DEC
FROM ORDER_CALENAR