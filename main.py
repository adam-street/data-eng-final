#
# paul_moua_final_project
# 01/07/2022

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions

# transform my data
class TransformData(beam.DoFn):
    def process(self, element):
        # print(element)

        yield element

# where my function will happen
def run():

    # location of pipeline
    p_opt = PipelineOptions(
        temp_location="gs://pm_new_project/temp/",
        project="york-cdf-start",
        region="us-central1",
        staging_location="gs://pm_new_project/staging",
        job_name="paul-moua-final-job",
        save_main_session=True
    )

    # the schemas for the three tables from BQ
    customers_schema = {
        'fields': [
            {'name': 'CUSTOMER_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'CUST_TIER_CODE', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        ]
    }

    orders_schema = {
        'fields': [
            {'name': 'CUSTOMER_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'TRANS_TM', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'ORDER_NBR', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'SKU', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'ORDER_AMT', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        ]
    }

    product_views_schema = {
        'fields': [
            {'name': 'CUSTOMER_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'EVENT_TM', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'SKU', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    }

    # new schemas that I will use when writing my data to bigquery
    new_schema1 = {
        'fields': [
            {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'sku', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'total_no_of_product_views', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        ]
    }

    new_schema2 = {
        'fields': [
            {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'sku', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'total_sales_amount', 'type': 'FLOAT', 'mode': 'REQUIRED'},
        ]
    }

    # reference tables of where my data should go
    ref_table1 = bigquery.TableReference(
        projectId="york-cdf-start",
        datasetId="final_paul_moua",
        tableId = "cust_tier_code-sku-total_no_of_product_views"
    )

    ref_table2 = bigquery.TableReference(
        projectId="york-cdf-start",
        datasetId="final_paul_moua",
        tableId = "cust_tier_code-sku-total_sales_amount"
    )

    # table references to help me write my data
    table1 = "york-cdf-start.final_input_data.customers"
    table2 = "york-cdf-start.final_input_data.orders"
    table3 = "york-cdf-start.final_input_data.product_views"


    with beam.Pipeline(runner="DataflowRunner", options=p_opt) as pipeline:
        data1 = pipeline | "ReadFromBigQuery" >> beam.io.ReadFromBigQuery(
            query='SELECT table1.CUST_TIER_CODE, table2.SKU, COUNT(table2.SKU) as total_no_of_product_views FROM `york-cdf-start.final_input_data.customers` as table1 ' \
                  'JOIN `york-cdf-start.final_input_data.product_views` as table2 ON table1.CUSTOMER_ID = table2.CUSTOMER_ID ' \
                  'GROUP BY table1.CUST_TIER_CODE, table2.SKU',
            use_standard_sql=True,
        )
        # data1 | "Print" >> beam.Map(print)
        data2 = pipeline | "ReadFromBigQuery2" >> beam.io.ReadFromBigQuery(
            query='SELECT table4.CUST_TIER_CODE, table5.SKU, ROUND(SUM(table5.ORDER_AMT), 2) as total_sales_amount FROM `york-cdf-start.final_input_data.customers` as table4 '\
                  'JOIN `york-cdf-start.final_input_data.orders` as table5 ON table4.CUSTOMER_ID = table5.CUSTOMER_ID '\
                    'GROUP BY table4.CUST_TIER_CODE, table5.SKU',
            use_standard_sql=True,
        )
        # data2 | "Print" >> beam.Map(print)

        data1 | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            ref_table1,
            schema=new_schema1,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location="gs://pm_new_project/temp"
        )

        data2 | "Write to BigQuery2" >> beam.io.WriteToBigQuery(
            ref_table2,
            schema=new_schema2,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location="gs://pm_new_project/temp"
        )

        
        pass

if __name__ == '__main__':
    print("done")
    run()
    pass
