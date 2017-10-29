import datetime

import unittest

def fun(x):
    return x + 1

class MyTest(unittest.TestCase):
    def test(self):
        self.assertEqual(fun(3), 4)
        self.assertTrue("12012017")
        self.assertFalse("12002017")
        self.assertFalse("12002017")
        self.assertFalse("00002017")
        self.assertFalse("12012017")


def validateDate(date_text):
    try:
        datetime.datetime.strptime(date_text, '%m%d%Y')
        return True
    except ValueError:
        return False
        #raise ValueError("Incorrect data format, should be mmddYYYY")


print(validateDate("01122017"))



def getMedianFromTheList(list):
    if(list is None or len(list) == 0):
        return  0

    list_length = len(list)

    if(list_length %2 == 0):
        index = int(list_length / 2)
        #print(index)
        median = (list[index] + list[index - 1]) / 2
        return round(median)
    else:
        index = list_length // 2
        return list[index]




#print(getMedianFromTheList([2, 3]))
#print(getMedianFromTheList([2, 3, 4]))
#print(getMedianFromTheList([2,3,4,5,6]))
#print(getMedianFromTheList([2,3,4,5,6,7,8]))

# print(getMedianFromTheList([2,3]))
# print(getMedianFromTheList([2,2]))
#
# print(getMedianFromTheList([2,2,3,4]))
# print(getMedianFromTheList([2,2,4,4]))
#
# print(getMedianFromTheList([2,2,3,6,7,4]))
# print(getMedianFromTheList([2,2,3,4,4,6]))


#print(getMedianFromTheList([2,3,3,4]))
#print(getMedianFromTheList([2,3,4,4]))
#print(getMedianFromTheList([2,3,4,5,6]))
#print(getMedianFromTheList([2,3,4,6,5,5]))
#print(getMedianFromTheList([2,3,4,5,4,4]))





#print(round(12.2))
#list1= [1,2]








#print(list)