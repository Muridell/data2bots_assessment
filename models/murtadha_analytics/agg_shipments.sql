/*ingestion_date date NOT NULL
tt_late_shipments int NOT NULL
tt_undelivered_shipments
*/
SELECT  DISTINCT DATE(SYSDATE()) INGESTION_DATE,
(SELECT * FROM {{ref('late_shipments')}}) TT_LATE_SHIPMENTS,
(SELECT * FROM {{ref('undelivered_shipments')}}) TT_UNDELIVERED_SHIPMENTS
FROM {{source('s3', 'SHIPMENT_DELIVERIES')}}