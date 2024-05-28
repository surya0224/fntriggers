import random
from copy import *
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def mintominfareladder(fareladder1,fareladder2):
    # - - - - - - Convert fares to integer so that discrepancy of index found in other list is solved
    # - - - for eg. ladder1 = [123.45]
    #  - - -        fareladder1 = [123.4], then code will give error.
    fareladder1 = [int(fare) for fare in fareladder1]
    fareladder2 = [int(fare) for fare in fareladder2]
    fareladder11 = deepcopy(fareladder1)
    fareladder22 = deepcopy(fareladder2)
    fareladder11.sort()
    fareladder22.sort()
    mapping1= dict(zip(fareladder11,fareladder22)) # mapped two list
    #printing the fareladder1 and corresponding elemnt in fareladder 2 and if element is not mapped then printing -1 along with it
    list1=[]
    for x in fareladder1:
         if x in mapping1:
             a = mapping1[x]
             print x,fareladder2.index(a)
             list1.append(fareladder2.index(a))
         else:
             print x , -1
             list1.append(-1)
    # # print fareladder1 , list1
    mapping2= dict(zip(fareladder22,fareladder11))
    list2=[]
    for y in fareladder2:
         if y in mapping2:
             b=mapping2[y]
             print y, fareladder1.index(b)
             list2.append(fareladder1.index(b))
         else:
             print y, -1
             list2.append(-1)
    out = [list1,list2]
    return out


if __name__=='__main__':
    fl1 = [108000.52, 140000.45]
    fl2 = [250000.32,400000.65, 300000.2156]
    print mintominfareladder(fl1, fl2)
