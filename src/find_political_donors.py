import os.path
import sys
import traceback
import datetime
import csv


TOKEN = "_"


#This value stores the customer_info, date, median, totalNo, No of times
receipient_date_Map = {}


'''
This distionary is represented as
Key = CustomerID + "_" = ZipCode = C1000000_12345
Each Key will contain [transaction_amt, [list_of_transaction_amt_in_sorted_manner]]
'''
cust_zip_key = {}

# To save the lines of zip_code for saving in txt file, each line consist of
# [cmte_id, zip_code, median, no_of_customers, total_transaction]
list_of_cust_median_by_zipcode = []

buffer_lines = 100 # USed for streaming

#File Names obtained from bash_command
input_filename = ""
output_zip_filename = ""
output_date_filename = ""

# Reads the input file and parses it in list of lines
def read_input_file():
    global input_filename
    clear_all_dataset_and_files()
    file = None
    try:
        file = open(input_filename)
        content = file.readlines()
        content = [x.strip() for x in content]
        select_few_lines_as_streaming(content)
    except IOError as exp:
        print("ERROR: Unable to open the file " + input_filename + " " + traceback.format_exc())
    finally:
        if file is not None:
            file.close()


# This method is called to clear all the global variables used and clears the output file
def clear_all_dataset_and_files():
    global cust_zip_key, list_of_cust_median_by_zipcode, receipient_date_Map
    cust_zip_key = {}
    list_of_cust_median_by_zipcode = []
    receipient_date_Map = {}
    clear_data(output_zip_filename)
    clear_data(output_date_filename)


# Checks if the string can be converted into int or not
def represents_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


# This method acts as a stream of lines input whose value depends on buffer_input variable
def select_few_lines_as_streaming(content):
    if (content is None or len(content) == 0):
        return
    no_of_lines=len(content)
    start_pointer = 0
    end_pointer = start_pointer + buffer_lines
    while (end_pointer <= no_of_lines):
        verify_content(content[start_pointer:end_pointer])
        start_pointer = end_pointer
        end_pointer = start_pointer + buffer_lines
    verify_content(content[start_pointer: no_of_lines])



# This method verifies whether the input lines are in proper format or not and forward it for urther processing
def verify_content(content):
    if(content is None or len(content) == 0):
        return
    global list_of_cust_median_by_zipcode

    list_of_cust_median_by_zipcode = []
    for line in content:
        is_valid_for_median_by_zip_cal = True
        is_valid_for_median_by_date_cal = True

        splitList = line.split("|")
        # No of elements present in the splitList
        if len(splitList) != 21:
           # print(len("Error less no of elements"))
            continue

        cmte_id = splitList[0].strip()
        zip_code = splitList[10].strip()
        transaction_dt = splitList[13].strip()
        others = splitList[15].strip()
        transaction_amt = splitList[14].strip()

        if transaction_amt == "" or represents_int(transaction_amt) == False:
            continue
        else:
            transaction_amt = int(transaction_amt)

        if others != "" or cmte_id == "" or transaction_amt == "" :
            continue

        if len(zip_code) < 5:
            is_valid_for_median_by_zip_cal = False
        else:
            zip_code = zip_code[0:5]

        if len(transaction_dt) < 8:
            is_valid_for_median_by_date_cal = False
        else:
            is_valid_for_median_by_date_cal = validate_date(transaction_dt)

        if is_valid_for_median_by_zip_cal:
            cal_median_by_zipcode((cmte_id, zip_code, transaction_amt))

        if is_valid_for_median_by_date_cal:
            cal_median_by_date(cmte_id, transaction_amt, transaction_dt)

    store_median_by_zipcode()
    store_median_by_date()


# It calculates the median and total trasaction for the customer on the corresponding zipcode
def store_and_calc_median_total_transaction(splitList):
    cmte_id, zip_code, transaction_amt = splitList
    global cust_zip_key
    key = cmte_id + TOKEN + zip_code
    if key in cust_zip_key:
        # Key is present so add
        stored_data = cust_zip_key[key]
        stored_data[0] = stored_data[0] + transaction_amt
        transaction_list = add_element_to_list_in_asc_order(transaction_amt, stored_data[1])
        return (stored_data[0], get_median_from_list(transaction_list), len(transaction_list))
    else:
        cust_zip_key[key] = [transaction_amt, [transaction_amt]]
        return (transaction_amt, transaction_amt, 1)


# It saves the median and total transaction information in the list_of_cust_median_by_zipcode
def cal_median_by_zipcode(splitList):
    cmte_id, zip_code, transaction_amt = splitList
    total_transaction, median, no_of_customers = store_and_calc_median_total_transaction(splitList)
    #print([cmte_id, zip_code, median, no_of_customers, total_transaction])
    list_of_cust_median_by_zipcode.append([cmte_id, zip_code, median,no_of_customers, total_transaction])



# Calculates median and total transaction for the customer of specific date
def cal_median_by_date(cmte_id, transaction_amt, transaction_dt):
    key = transaction_dt + TOKEN + cmte_id
    global receipient_date_Map
    if (key in receipient_date_Map):
        stored_data = receipient_date_Map[key]
        transaction_list = stored_data[3]
        receipient_date_Map[key] = [cmte_id, transaction_dt, transaction_amt + stored_data[2],
                                    add_element_to_list_in_asc_order(transaction_amt, transaction_list)]

    else:
        receipient_date_Map[key] = [cmte_id, transaction_dt, transaction_amt, [transaction_amt]]


# Stores the zipcode data in the output_zip_filename
def store_median_by_zipcode():
    append_content(output_zip_filename, list_of_cust_median_by_zipcode)


# fetches all the data stored in receipient_date_Map and arranges it on the basis of customer id and date
def store_median_by_date():
    list_of_cust_median_by_date = []
    for key, value in sorted(receipient_date_Map.items(), key=lambda item: (item[1][0], item[1][1])):
        cmte_id = value[0]
        transaction_dt = value[1]
        total_transaction = value[2]
        transaction_amt_list = value[3]
        no_of_cmte_id = len(transaction_amt_list)
        median = get_median_from_list(transaction_amt_list)
        list_of_cust_median_by_date.append([cmte_id,transaction_dt, median, no_of_cmte_id, total_transaction])

    write_content(output_date_filename,list_of_cust_median_by_date)



# If fileName doesnt begin with ./
def is_file_path_begins_in_same_working_directory(filepath):
    #print(filepath)
    if(len(filepath) < 2):
        return False

    if (filepath[:2] != "./"):
        print("Output filepath is invalid it should begin with ./")
        return False
    return True

# If the output file is not present then create an output file
def check_filepath_and_create_if_not_exist(filepath):
    basedir = os.path.dirname(filepath)
    if not os.path.exists(basedir):
        print("Info: Output Folder doesn't exist creating ")
        os.makedirs(basedir)

    if not os.path.isfile(filepath):
        with open(filepath, 'w') as file:
            pass


# Checks if we are getting all the parameters from the bash input or not
def check_bash_shell_input(argv):
    # filePath = os.getcwd()
    # print(filePath)
    global input_filename, output_date_filename, output_zip_filename


    if(not is_file_path_begins_in_same_working_directory(argv[1]) or
        not is_file_path_begins_in_same_working_directory(argv[2]) or
        not is_file_path_begins_in_same_working_directory(argv[3])):
     print("Passing parameters should begin with ./")
     return False

    input_filename = argv[1][2:]
    output_zip_filename = argv[2][2:]
    output_date_filename = argv[3][2:]

    # input_filename = "input/itcont.txt"
    # output_zip_filename = "output/medianvals_by_date.txt"
    # output_date_filename = "output/medianvals_by_zip.txt"

    if (not os.path.isfile("./"+input_filename)):
        print("Input file is not present")
        return False

    check_filepath_and_create_if_not_exist(output_zip_filename)
    check_filepath_and_create_if_not_exist(output_date_filename)

    return True


# Validates if input is in format mmddYYYY
def validate_date(date_text):
    try:
        datetime.datetime.strptime(date_text, '%m%d%Y')
        return True
    except ValueError:
        return False


# Deletes the content of the file
def clear_data(filepath):
    if filepath is None or filepath == "" :
        return

    with open(filepath, 'w') as file:
        pass


# Writes the list data in the file
def write_content(filepath, list):
    if filepath is None or filepath == "" or list is None or len(list) == 0 :
        return

    with open(filepath, 'w') as file:
        for row in list:
            result = ""
            for single_element in row:
                result += str(single_element) + '|'
            result = result[:-1]
            file.write(result)
            file.write('\n')



# Appends the list data in the file
def append_content(filepath, list):

    if filepath is None or filepath == "" or list is None or len(list) == 0 :
        return

    with open(filepath, 'a') as file:
        for row in list:
            result = ""
            #print(row)
            if(len(row) == 0):
                continue
            for single_element in row:
                result += str(single_element) + '|'
            result = result[:-1]
            file.write(result)
            file.write('\n')

import math

# This method returns the median of the list assuming list is in ascending order
def get_median_from_list(list):
    if (list is None or len(list) == 0):
        return 0

    list_length = len(list)

    if (list_length % 2 == 0):
        index = int(list_length / 2)

        median = (float(list[index] + list[index - 1])) / float(2)
        ceil = math.ceil(median)
        floor = math.floor(median)

        # print(median)
        if(median < floor+0.5):
            return floor
        else:
            return ceil
        # print(round(a,1))
        # return(round(a,1))
    else:
        index = list_length // 2
        return list[index]


# Adds element in the list in sorted manner
def add_element_to_list_in_asc_order(element, list):
    if list is None:
        list = []
    isAdded = False
    for i in range(len(list)):
        if (list[i] >= element):
            list.insert(i, element)
            isAdded = True
            break

    if (isAdded == False):
        list.append(element)
    return list

'''
This method will be the starting point when called from the
'''
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("\nInvalid Command \n"
              "------------------ \n"
              "The command should be in following format \n"
              "python <PATH of find_political_donors.py> <Input_file_name_with_path> <Output_ZIP_File_name_with_path> <Output_Date_File_name_with_path>"
            "\nExample: \n"
            "python ./src/find_political_donors.py ./input/itcont.txt ./output/medianvals_by_zip.txt ./output/medianvals_by_date.txt"
              "\n----------------")
    else:
        if(check_bash_shell_input(sys.argv) == True):
            #print("Inside the bash_shell")
            read_input_file()


#read_input_file()