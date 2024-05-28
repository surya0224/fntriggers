import random
from copy import *
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def mintomin(fareladder1, fareladder2):
    '''
    Function for mapping the fareladder1 and fareladder2
    Args:
        fareladder1(list)   :list of fares
        fareladder2(list)   :list of fares
    Returns:
        out(list of 2 lists):
                            out[0] - list of indexes information of fareladder2 
                                    in the indexes of fareladder1 to which they 
                                    are mapped
                            out[1] - list of indexes information of fareladder1 
                                    in the indexes of fareladder2 to which they 
                                    are mapped
    Note:
        A value of -1 in the out list signifies No mapping has occured for that fare
    '''
    fareladder11 = deepcopy(fareladder1)
    fareladder22 = deepcopy(fareladder2)
    print fareladder11, fareladder22
    for i in range(len(fareladder11)):
        fareladder11[i] = deepcopy(round(fareladder11[i]))
    for i in range(len(fareladder22)):
        fareladder22[i] = deepcopy(round(fareladder22[i]))
    print fareladder11, fareladder22
    fareladder11.sort()
    fareladder22.sort()
    print fareladder11, fareladder22
    mapping1 = dict(zip(fareladder11, fareladder22))  # mapped two list
    # printing the fareladder1 and corresponding elemnt in fareladder 2 and if element is not mapped then printing -1 along with it
    list1 = []
    for x in fareladder1:
        if x in mapping1:
            a = mapping1[x]
            print x, fareladder2.index(a)
            list1.append(fareladder2.index(a))
        else:
            print x, -1
            list1.append(-1)
    # # print fareladder1 , list1
    mapping2 = dict(zip(fareladder22, fareladder11))
    list2 = []
    for y in fareladder2:
        if y in mapping2:
            b = mapping2[y]
            print y, fareladder1.index(b)
            list2.append(fareladder1.index(b))
        else:
            print y, -1
            list2.append(-1)
    out = [list1, list2]
    return out

if __name__=='__main__':
    fl1 = [1.5, 2.5, 4.5, 3.4, 2.2]
    fl2 = [2.2, 3.3, 4, 2.4, 2.5, 2.2, 1]
    result = mintomin(fl1, fl2)
    print result
