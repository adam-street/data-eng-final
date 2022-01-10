import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery


# define constants
PROJECT_ID = "york-cdf-start"
IN_DATASET_ID = "final_input_data"
IN_TABLE_CUSTOMERS = "customers"
IN_TABLE_PRODUCT_VIEWS = "product_views"
IN_TABLE_ORDERS = "orders"

OUT_DATASET_ID = "final_archana_ganipineni"
OUT_TABLE_CUST_PROD_VIEWS = "cust_tier_code-sku-total_no_of_product_views"
OUT_TABLE_CUST_SALES = "cust_tier_code-sku-total_sales_amount"

TEMP_LOCATION = "gs://york_temp_files/tmp"
STAGING_LOCATION = "gs://york_temp_files/staging"
REGION = "us-central1"
JOB_NAME = "archana-ganipineni-final-job"
RUNNER = "DataflowRunner"


dest_schema_cust_prod_views = {
    'fields': [
        {"name": "cust_tier_code", "type": "STRING", "mode": "REQUIRED"},
        {"name": "sku", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "total_no_of_product_views", "type": "INTEGER", "mode": "REQUIRED"}
    ]
}

dest_schema_cust_sales = {
    'fields': [
        {"name": "cust_tier_code", "type": "STRING", "mode": "REQUIRED"},
        {"name": "sku", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "total_sales_amount", "type": "FLOAT", "mode": "REQUIRED"}
    ]
}

dest_table_cust_prod_views = bigquery.TableReference(
    projectId=PROJECT_ID,
    datasetId=OUT_DATASET_ID,
    tableId=OUT_TABLE_CUST_PROD_VIEWS
)

dest_table_cust_sales = bigquery.TableReference(
    projectId=PROJECT_ID,
    datasetId=OUT_DATASET_ID,
    tableId=OUT_TABLE_CUST_SALES
)


def run():
    parser = argparse.ArgumentParser()
    args, beam_args = parser.parse_known_args()

    beam_options = PipelineOptions(
        beam_args,
        runner='DataflowRunner',
        project=PROJECT_ID,
        job_name=JOB_NAME,
        temp_location=TEMP_LOCATION,
        staging_location=STAGING_LOCATION,
        region=REGION)

    local_beam_options = PipelineOptions(
        beam_args,
        project=PROJECT_ID,
        job_name=JOB_NAME,
        temp_location=TEMP_LOCATION,
        staging_location=STAGING_LOCATION,
        region=REGION
    )

    pipeline_options = PipelineOptions(
        project=PROJECT_ID,
        region=REGION,
        temp_location=TEMP_LOCATION,
        staging_location=STAGING_LOCATION,
        job_name=JOB_NAME,
        save_main_session=True
    )
    with beam.Pipeline(options=pipeline_options) as p:
        data_from_prod_views = p | "Read data from customers & prod views" >> beam.io.ReadFromBigQuery(
            query="SELECT c.cust_tier_code, p.sku, count(*) AS total_no_of_product_views "
                    "FROM final_input_data.product_views AS p "
                    "JOIN final_input_data.customers AS c "
                    "ON p.customer_id = c.customer_id "
                    "GROUP BY c.cust_tier_code, p.sku ",
                    project=PROJECT_ID,
            use_standard_sql=True)  # | "print results prod views" >> beam.Map(print)

        data_from_sales = p | "Read data from customers & sales" >> beam.io.ReadFromBigQuery(
            query="SELECT c.cust_tier_code, o.sku, CAST(sum(o.ORDER_AMT) AS NUMERIC) AS total_sales_amount "
                    "FROM `york-cdf-start.final_input_data.orders` AS o "
                    "JOIN `york-cdf-start.final_input_data.customers` AS c "
                    "ON o.customer_id = c.customer_id "
                    "GROUP BY c.cust_tier_code, o.sku ",
                    project=PROJECT_ID,
            use_standard_sql=True)  # | "print results sales" >> beam.Map(print)

        # write to prod views table
        data_from_prod_views | "Write data to prod views table" >> beam.io.WriteToBigQuery(
            dest_table_cust_prod_views,
            schema=dest_schema_cust_prod_views,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # write to sales table
        data_from_sales | "Write data to sales table" >> beam.io.WriteToBigQuery(
            dest_table_cust_sales,
            schema=dest_schema_cust_sales,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('reading and writing to BigQuery')
    run()
