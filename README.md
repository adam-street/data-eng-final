report link: https://datastudio.google.com/s/piBAcVy4-1c

# Project Overview

 1. Use apache beam to create a data pipeline to aggregate data from three big query input tables.
 2. Use Jenkins to launch a new Google Dataflow job anytime code is merged into the main branch of your repo
 3. Use the newly aggregated data to create a Google Data Studio report to visualize the data.

# General Information
  - All projects that you create should be under the google cloud project id ```york-cdf-start```
  - Take care to protect any keys or credentials so that they are not exposed to the public
  - Follow the specifications and observe all specified data types.  Transform data as required
  - SKU is an industry standard unique product number used to identify a particular product and version
  - The data was randomly generated and may not reflect real world operations.  Do not worry that some values may be duplicated by chance. This will not impact the reports as specified.

	## Help Parameters
	- Help will be limited to assistance with systems problems or clarifications Bob deams neccesary (that were not clear in the specs)
	- Student to student communications are not permissable.  You may not answer a question by another student
	- All requests for assistance or clarification will be through the class discord server and will be answered within 1 hour by Taylor, Nadhiya, Bob, or Adam.
	- Do not post any code or fragments on discord.  Just the general error or situation.
	- If more detail is required, we will ask you do send it in an email to the four of us above.  Do not copy others.

  
    ## Input Data Specs
    - The input data is stored in three big query tables under the ```york-cdf-start.final_input_data``` dataset
	- CUST_TIER_CODE 0 = free tier, 1 = paid tier

      ### customers

      | Field Name                | Type           |
      | ------------------------- | -------------- |
      | CUSTOMER_ID               | STRING         |
      | CUST_TIER_CODE            | INTEGER        |

      ### product_views
      | Field Name                | Type           |
      | ------------------------- | -------------- |
      | CUSTOMER_ID               | STRING         |
      | EVENT_TM                  | TIMESTAMP      |
      | SKU                       | STRING         |

      ### orders
      | Field Name                | Type           |
      | ------------------------- | -------------- |
      | CUSTOMER_ID               | STRING         |
      | TRANS_TM                  | TIMESTAMP      |
      | ORDER_NBR                 | STRING         |
      | SKU                       | INTEGER        |
      | ORDER_AMT                 | FLOAT          |

     ## Output Data Specs
    - Your pipeline should output the following tables under a dataset with the following naming convention ```york-cdf-start.final_[first-name]_[last-name]```

      ### cust_tier_code-sku-total_no_of_product_views

      | Field Name                | Type           | Mode        |
      | ------------------------- | -------------- | ----------- |
      | cust_tier_code            | STRING         | REQUIRED    |
      | sku                       | INTEGER        | REQUIRED    |
      | total_no_of_product_views | INTEGER        | REQUIRED    |

      ### cust_tier_code-sku-total_sales_amount

      | Field Name                | Type           | Mode        |
      | ------------------------- | -------------- | ----------- |
      | cust_tier_code            | STRING         | REQUIRED    |
      | sku                       | INTEGER        | REQUIRED    |
      | total_sales_amount        | FLOAT          | REQUIRED    |
  
## Pipeline Details
  - Your pipeline should include a ```requirements.txt```file with all of the required python packages listed inside
  - Your pipeline should take the following input options
  
  
      | Name             | Default                            | Description                                       |
      | ---------------- | ---------------------------------- | ------------------------------------------------- |
      | project          | york-cdf-start                     | Project ID                                        |
      | region           | us-central1                        | Regional endpoint name                            |
      | runner           | DataflowRunner                     | Pipeline runner that executes your pipeline       |
      | temp_location    | gs://york_temp_files/tmp           | Cloud Storage path for Dataflow temporary files   |
      | staging_location | gs://york_temp_files/staging       | Cloud Storage path for Dataflow staging files     |
      | job_name         | [first-name]-[last-name]-final-job | Display name for dataflow job                     |
   
   
  - Your pipeline should pull all the data from the following input tables
    - ```york-cdf-start.final_input_data.customers```
    - ```york-cdf-start.final_input_data.product_views```
    - ```york-cdf-start.final_input_data.orders```
   
  - Your pipeline should then merge data from the input tables to create the following tables
    - cust_tier_code-sku-total_no_of_product_views
    - The data in this table should be the result of grouping all of the records from the ```york-cdf-start.final_input_data.product_views``` table by the customers' tier code, the products SKU, and the count of records that match the tier code and SKU combination.
    - cust_tier_code-sku-total_sales_amount
    - The data in this table should be the result of grouping all of the records from the ```york-cdf-start.final_input_data.orders``` table by the customers' tier code, the products SKU, and the sum of the order amount for records that match the tier code and SKU combination.

## Report Details
  - Your report should include the following elements
    - A sum of all product views
    - A sum of all sales
    - A sum of free tier customer's product views
    - A sum of free tier customer's sales
    - A sum of paid tier customer's product views
    - A sum of paid tier customer's sales
    - A column chart showing the free tier customer's product views and sales next to the paid tier customer's product views and sales
    - A bar chart showing total sales for each SKU (sorted descending)
    - A bar chart showing total sales for each SKU for free tier customers  (sorted descending)
    - A bar chart showing total sales for each SKU for paid tier customers  (sorted descending)

- Designing the layout and style of the report is up to you. Find the best way you can to display the requested data to make it readable and easy to digest.

## Jenkins Details
 - Create a jenkins build project with the naming convention ```[first-name]-[last-name]-final-project``` inside the ```/var/dataflow``` directory
   - Trigger the build anytime there is a push to your ```main``` branch
   - ** Note ** Only one Dataflow job of a given job_name can be running at a time. Batch jobs in Dataflow can take a while (up to 10 - 15 minutes), so let it finish before making a new GitHub commit or running a new job with the same name.
   - Create and activate a python venv inside your new final-project folder
   - Install required python packages inside your new venv from your ```requirements.txt``` file
   - Set your ```GOOGLE_APPLICATION_CREDENTIALS``` environment variable to ```/var/dataflow/york-cdf-start-b0964900c176.json```
   - Start your pipeline
