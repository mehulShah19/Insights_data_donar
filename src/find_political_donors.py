import os.path
import errno
import sys
import traceback
import datetime
import csv


TOKEN = "_"



#This value stores the customer_info, date, median, totalNo, No of times
receipient_date_Map = {}


'''
This distionary is represented as
CustomerID = C1000000
ZipCode = 12345
Key = CustomerID + "_" = ZipCode = C1000000_12345
Each Key will contain
Total Contribution Count
And List which stores value in sorting order
'''
cust_zip_key = {}
list_of_cust_median_by_zipcode = []

input_filename = ""
output_zip_filename = ""
output_date_filename = ""

def read_input_file():
    global input_filename
    print(input_filename)
    clear_all_dataset_and_files()
    file = None
    try:
        file = open(input_filename)
        content = file.readlines()
        content = [x.strip() for x in content]
        parse_and_verify_content(content)
    except IOError as exp:
        print("ERROR: Unable to open the file " + input_filename + " " + traceback.format_exc())
    finally:
        if file is not None:
            file.close()



def clear_all_dataset_and_files():
    global cust_zip_key, list_of_cust_median_by_zipcode, receipient_date_Map
    cust_zip_key = {}
    list_of_cust_median_by_zipcode = []
    receipient_date_Map = {}
    clear_data(output_zip_filename)
    clear_data(output_date_filename)



def parse_and_verify_content(content):
    for line in content:
        is_valid_for_median_by_zip_cal = True
        is_valid_for_median_by_date_cal = True

        splitList = line.split("|")
        # No of elements present in the splitList
        if len(splitList) != 21:
            print(len("Error less no of elements"))
            continue

        cmte_id = splitList[0]
        zip_code = splitList[10]
        transaction_dt = splitList[13]
        transaction_amt = int(splitList[14])
        others = splitList[15]

        if others != "":
            continue

        if cmte_id == "" or transaction_amt == "":
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



def cal_median_by_zipcode(splitList):
    cmte_id, zip_code, transaction_amt = splitList
    total_transaction, median, no_of_customers = store_and_calc_median_total_transaction(splitList)
    print([cmte_id, zip_code, median, no_of_customers, total_transaction])
    list_of_cust_median_by_zipcode.append([cmte_id, zip_code, median,no_of_customers, total_transaction])



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



def store_median_by_zipcode():
    append_content(output_zip_filename, list_of_cust_median_by_zipcode)



def store_median_by_date():
    list_of_cust_median_by_date = []
    for key, value in sorted(receipient_date_Map.items(), key=lambda item: (item[1][0], int(item[1][1]))):
        cmte_id = value[0]
        transaction_dt = value[1]
        total_transaction = value[2]
        transaction_amt_list = value[3]
        no_of_cmte_id = len(transaction_amt_list)
        median = get_median_from_list(transaction_amt_list)
        list_of_cust_median_by_date.append([cmte_id,transaction_dt, median, no_of_cmte_id, total_transaction])

    write_content(output_date_filename,list_of_cust_median_by_date)



def check_filepath_and_create_if_not_exist(filepath):
    if not os.path.exists(os.path.dirname(filepath)):
        try:
            os.makedirs(os.path.dirname(filepath))
            if (not os.path.isfile(filepath)):
                return False
            return True
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                return False
        except Exception:
            return False

print(check_filepath_and_create_if_not_exist("/Users/edplus/Google Drive/Workspace/PythonWorkspace/Insight_Data_Donor/output/medianvals_by_zip.txt"))

def check_bash_shell_input(argv):
    filePath = os.getcwd()
    print(filePath)
    input_filename = filePath + argv[1][1:]
    output_zip_filename = filePath + argv[2][1:]
    output_date_filename = filePath + argv[3][1:]

    print(input_filename)
    print(output_zip_filename)
    print(output_date_filename)

    if (not os.path.isfile(input_filename)):
        print("Input file is not present")
        return False

    if (not check_filepath_and_create_if_not_exist(output_zip_filename)):
        print("Output zip file path is inavalid")
        return False

    if (not check_filepath_and_create_if_not_exist(output_date_filename)):
        print("Output date file path is invalid")
        return False

    return True


'''
This method will be the starting point when called from the
'''
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("\nInvalid Command \n"
              "------------------ \n"
              "The command should be in following format \n"
              "python <PATH of find_political_donors.py> <Input_file_name_with_path> <Output_ZIP_File_name_with_path> <Output_Date_File_name_with_path>"
            "\n Example \n"
            "python ./src/find_political_donors.py ./input/itcont.txt ./output/medianvals_by_zip.txt ./output/medianvals_by_date.txt"
              "\n----------------")
    else:
        if(not check_bash_shell_input(sys.argv)):
            read_input_file()



def validate_date(date_text):
    try:
        datetime.datetime.strptime(date_text, '%m%d%Y')
        return True
    except ValueError:
        return False



def clear_data(filepath):
    if filepath is None or filepath == "" :
        return

    with open(filepath, 'w') as file:
        pass



def write_content(filepath, list):
    if filepath is None or filepath == "" or list is None or len(list) == 0 :
        return

    with open(filepath, 'w') as csv_file:
        writer = csv.writer(csv_file, delimiter='|')
        for row in list:
            writer.writerow(row)



def append_content(filepath, list):
    if filepath is None or filepath == "" or list is None or len(list) == 0:
        return

    with open(filepath, 'a') as csv_file:
        writer = csv.writer(csv_file, delimiter='|')
        for row in list:
            writer.writerow(row)


def get_median_from_list(list):
    if (list is None or len(list) == 0):
        return 0

    list_length = len(list)

    if (list_length % 2 == 0):
        index = int(list_length / 2)

        median = (list[index] + list[index - 1]) / 2
        return round(median)
    else:
        index = list_length // 2
        return list[index]


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


