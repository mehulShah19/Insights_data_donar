# Find Political Donors

This project parses the donor's data input file and generates 
two output file one on the basis of customer_id and zipcode()medianvals_by_zip.txt) and other on customer_id and date medianvals_by_date.txt and calculates the median and total transaction for the input value
 

Every input line is forwarded only if following conditions are satisfied
1. No_of_fields_seperated_by_delimiter('|') = 21
2. Customer_id, transaction_amt - is not empty
4. transaction_amt = should be in int
3. others field - should be empty
5. zip_code = more than 5 in length(to calculate medianvals_by_zip)
6. transaction_date = should be in format("mmddYYYY")( to calculate medianvals_by_date)

### Median Values by Zip
For calculating Median and total transaction on the basis of zipcode and customer_id

A dictionary(cust_zip_key) is created whose key is the unique combination of customer_id and zip_code
This dictionary contains [total_tranasaction , list_of transaction_in_ascending_order]

Also, a list(list_of_cust_median_by_zipcode) is maintained which stores all the parsed data for the streaming input.
It used cust_zip_key dictionary for calculating median and total_transaction

When a single line is inserted 
--> It validates the input 
--> saves the information on the dictionary 
--> appends the calculated values like total transaction and median for specific customer from the dictionary to the list

Once the streaming data is over, the saved list is appended on the file medianvals_by_zip.txt and all the data of the list is flushed


## Median Values by Date
For calculating Median and total Transaction on the bass of date and customer_id

A dictionary(receipient_date_Map) is created whose key is unique combination of date and customer id
It stores all the following information customer_info, list_of transaction_in_ascending_order

When a single line is inserted 
--> It validates the input 
--> saves the information on the dictionary

Whenever we want the output, store_median_by_date() method is called. 
It calculates median and total transaction on each key value of the dictionary.
Later on, it arranges the data in sorted order of customer_id and transaction_date and stores the data in the medianvals_by_date.txt file


### Streaming
For streaming, currently, buffer_input variable with value 100 is being used. Once, 100 lines are received, we forward the data for parsing. It's being tested for buffer value = 3
In real-world scenario, time can be considered as a factor for streaming, every x seconds(depending of application), data gathering can be forwarded for parsing and storing it





