import random
import statistics
from copy import deepcopy
from pyomo.environ import *
from pyomo.opt import SolverFactory
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


opt = SolverFactory("glpk")
THRESHOLD_PERCENT = 50.0


@measure(JUPITER_LOGGER)
def process_results(results, fareladder1, fareladder2):
    sol = results["Solution"].__dict__["_list"][0]["Variable"]
    nzdict = {}  # delete the zero binary variables from sol and store in nzdict
    for key, value in sol.items():
        if abs(value["Value"]) >= 0.001:
            nzdict[key] = value["Value"]
    out1 = [-1 for i in range(len(fareladder1))]
    out2 = [-1 for i in range(len(fareladder2))]
    for key in nzdict.keys():  # key is like 'A[0,1]'
        leftbox = key.split("[")[1]  # leftbox is like '0,1]'
        rightbox = leftbox.split("]")[0]  # rightbox is like '0,1'
        left_right = rightbox.split(",")  # left_right is like ['0', '1']
        out2[int(left_right[0])] = int(left_right[1])
        out1[int(left_right[1])] = int(left_right[0])
    return out1, out2
# introducing norm as a argument to the optimizefareladdder function which
# can take three strings i.e mini, med and raw


@measure(JUPITER_LOGGER)
def optimizefareladder(norm, ladder1=[], ladder2=[]):
    fareladder1 = deepcopy(ladder1)
    fareladder2 = deepcopy(ladder2)
    switched = False
    min_host_fl = min(fareladder1)
    max_host_fl = max(fareladder1)
    fareladder2 = [fare for fare in fareladder2 if (fare > min_host_fl * (1 - THRESHOLD_PERCENT / 100.0)) and (
    fare < max_host_fl * (1 + THRESHOLD_PERCENT / 100.0))]
    if len(fareladder2) > 0:
        if len(fareladder2) > len(fareladder1):
            switched = True
            fareladder1, fareladder2 = fareladder2, fareladder1
        # normaliazation by minimum values of fareladders         #mini
        # normalization  by median values of farreladders         #med
        # passing program as it as with no change in fareladders  #raw
        if norm == "mini":  #
            min1 = min(fareladder1)
            min2 = min(fareladder2)
            fareladder1 = [x / min1 for x in fareladder1]
            fareladder2 = [x / min2 for x in fareladder2]
        elif norm == "med":
            fareladder11 = list(fareladder1)
            fareladder22 = list(fareladder2)
            fareladder11.sort()
            fareladder22.sort()
            med1 = statistics.median(fareladder11)
            med2 = statistics.median(fareladder22)
            fareladder1 = [x / med1 for x in fareladder1]
            fareladder2 = [x / med2 for x in fareladder2]
        elif norm == "raw":
            pass

        d = [[0 for x in range(len(fareladder1))] for x in range(len(fareladder2))]
        for i in range(len(fareladder2)):  # rows
            for j in range(len(fareladder1)):  # cols
                d[i][j] = abs(fareladder1[j] - fareladder2[i])

        infinity = float('inf')
        model = AbstractModel()
        model.INDEXES1 = Set(initialize=range(len(fareladder1)))
        model.INDEXES2 = Set(initialize=range(len(fareladder2)))
        model.A = Var(model.INDEXES2, model.INDEXES1, within=Binary)

        # CONSTRAINTS( Total len1+len2 constraints )
        # constraint for selecting one or none  element from every COLUMN


        @measure(JUPITER_LOGGER)
        def choose_1_or_none_element_from_columnj_rule(model, j):
            return sum(model.A[i, j] for i in model.INDEXES2) <= 1

        model.nvari = Constraint(model.INDEXES1, rule=choose_1_or_none_element_from_columnj_rule)

        # constraint for selecting one or none  element from every ROW #


        @measure(JUPITER_LOGGER)
        def choose_1_or_none_element_from_rowi_rule(model, i):
            return sum(model.A[i, j] for j in model.INDEXES1) == 1

        model.nvarj = Constraint(model.INDEXES2, rule=choose_1_or_none_element_from_rowi_rule)

        # OBJECTIVE FUNCTION


        @measure(JUPITER_LOGGER)
        def obj_rule(model):
            return sum(d[i][j] * model.A[i, j] for i in model.INDEXES2 for j in model.INDEXES1)

        model.tot = Objective(rule=obj_rule, sense=minimize)

        instance = model.create_instance()
        results = opt.solve(instance, tee=True)
        instance.solutions.store_to(results)
        out1, out2 = process_results(results, fareladder1, fareladder2)

        if switched:
            out2_new = [ladder2.index(fareladder1[i]) if i != -1 else -1 for i in out2]
            # print "\noriginal = ", [out2, out1]
            # print "\nconverted with index adla badli = ", [out2_new, out1]
            return [out2_new, out1]
        else:
            out1_new = [ladder2.index(fareladder2[i]) if i != -1 else -1 for i in out1]
            # print "\noriginal = ", [out1, out2]
            # print "\nconverted with Index adla badli = ", [out1_new, out2]
            return [out1_new, out2]
    else:
        out1 = [-1 for fare in range(len(ladder1))]
        out2 = [-1 for fare in range(len(ladder2))]
        # print [out1, out2]
        return [out1, out2]


if __name__ == '__main__':
    fareladder1 = [42290,38290,24790,34290,23790,26290,30290,48290,20600,19090]
    fareladder2 = [50456,37840,52325,39240,44850,33640,46719,35040,42981]


    print optimizefareladder('raw', fareladder1, fareladder2)
