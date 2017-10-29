
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


