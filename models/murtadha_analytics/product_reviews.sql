SELECT REVIEW,
        R.PRODUCT_ID,
        PRODUCT_NAME,
        PRODUCT_CATEGORY
FROM {{source('s3', 'REVIEWS')}} R
JOIN {{source('rds', 'DIM_PRODUCTS')}} P
ON R.PRODUCT_ID = P.PRODUCT_ID