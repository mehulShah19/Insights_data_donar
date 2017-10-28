import traceback
import datetime
import csv

TOKEN = "_"

'''
This value stores the customer_info, date, median, totalNo, No of times
'''
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


def read_input_file(fileName):
    clear_all_dataset()
    file = None
    try:
        file = open(fileName)
        content = file.readlines()
        content = [x.strip() for x in content]
        parse_and_verify_content(content)
    except IOError as exp:
        print("ERROR: Unable to open the file " + fileName + " " + traceback.format_exc())
    finally:
        if file is not None:
            file.close()


def clear_all_dataset():
    global cust_zip_key, list_of_cust_median_by_zipcode, receipient_date_Map
    cust_zip_key = {}
    list_of_cust_median_by_zipcode = []
    receipient_date_Map = {}


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


def validate_date(date_text):
    try:
        datetime.datetime.strptime(date_text, '%m%d%Y')
        return True
    except ValueError:
        return False
        # raise ValueError("Incorrect data format, should be mmddYYYY"


def getMedianFromTheList(list):
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


def store_and_calc_median_total_transaction(splitList):
    cmte_id, zip_code, transaction_amt = splitList
    global cust_zip_key
    key = cmte_id + TOKEN + zip_code
    if key in cust_zip_key:
        # Key is present so add
        stored_data = cust_zip_key[key]
        stored_data[0] = stored_data[0] + transaction_amt
        transaction_list = add_element_to_list_in_asc_order(transaction_amt, stored_data[1])
        return (stored_data[0], getMedianFromTheList(transaction_list), len(transaction_list))
    else:
        cust_zip_key[key] = [transaction_amt, [transaction_amt]]
        return (transaction_amt, transaction_amt, 1)



def cal_median_by_zipcode(splitList):
    cmte_id, zip_code, transaction_amt = splitList
    total_transaction, median, no_of_customers = store_and_calc_median_total_transaction(splitList)
    print([cmte_id, zip_code, str(median), str(no_of_customers), str(total_transaction)])
    list_of_cust_median_by_zipcode.append([cmte_id, zip_code, median,no_of_customers, total_transaction])



def add_element_to_list_in_asc_order(element, list):
    if list is None:
        list = []
    isAdded = False
    for i in range(len(list)):
        if (list[i] >= list):
            list.insert(i, element)
            isAdded = True
            break

    if (isAdded == False):
        list.append(element)
    return list



def cal_median_by_date(cmte_id, transaction_amt, transaction_dt):
    key = transaction_dt + TOKEN + cmte_id
    global receipient_date_Map
    if (key in receipient_date_Map):
        stored_data = receipient_date_Map[key]
        stored_data[2] = stored_data[2] + transaction_amt
        transaction_list = stored_data[3]
        receipient_date_Map[key] = [cmte_id, transaction_dt, transaction_amt,
                                    add_element_to_list_in_asc_order(transaction_amt, transaction_list)]

    else:
        receipient_date_Map[key] = [cmte_id, transaction_dt, transaction_amt, [transaction_amt]]

def store_median_by_zipcode():
        with open("medianvals_by_zip.txt", 'wb') as csv_file:
            writer = csv.writer(csv_file)
            for row in list_of_cust_median_by_zipcode:
                writer.writerow(row)



def store_median_by_date():
    list_of_cust_median_by_date = []
    for key, value in sorted(receipient_date_Map.items(), key=lambda item: (item[1][0], int(item[1][1]))):
        print(value)
        list_of_cust_median_by_date.append(value)


read_input_file("itcont.txt")
