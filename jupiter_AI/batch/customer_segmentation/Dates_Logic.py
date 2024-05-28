"""
Author: Prathyusha Gontla
End date of developement: 2018-04-28
Code functionality:
     Calculates no. of years, quarters, months, weeks, days in between 2 given dates.

MODIFICATIONS LOG
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :

"""

import datetime
import time
Q1 = [1, 2, 3]
Q2 = [4, 5, 6]
Q3 = [7, 8, 9]
Q4 = [10, 11, 12]
def quarter_logic(start_date,end_date):
    # start_time = datetime.datetime.now().time()

    import datetime
    import time
    import pandas as pd
    import numpy as np
    from dateutil.rrule import rrule, MONTHLY
    st =time.time()
    # start_date = datetime.date(2018, 3, 1)
    # end_date = datetime.date(2018, 12, 25)
    # start_date = start_date
    # end_date = end_date
    start_date1=start_date
    end_date1 =end_date
    start_date2=0
    end_date2=0

    quarters_to_remove = []
    years_to_remove=[]
    results={}

    def check_year(start_date, end_date):
        global records
        if start_date <= datetime.date(2018, 1, 1) and end_date >= datetime.date(2018, 12, 31):
            print "Y" + str(2018)
            years_to_remove.append(2018)
            results["Y" + "2018"] = datetime.date(2018, 1, 01).strftime("%Y-%m-%d"), datetime.date(2018, 12,
                                                                                                   31).strftime(
                "%Y-%m-%d")
        if start_date <= datetime.date(2019, 1, 1) and end_date >= datetime.date(2019, 12, 31):
            print start_date.year
            years_to_remove.append(2019)
            results["Y" + "2019"] = datetime.date(2019, 1, 01).strftime("%Y-%m-%d"), datetime.date(2019, 12,
                                                                                                   31).strftime(
                "%Y-%m-%d")
        if start_date <= datetime.date(2020, 1, 1) and end_date >= datetime.date(2020, 12, 31):
            print start_date.year
            years_to_remove.append(2020)
            results["Y" + "2020"] = datetime.date(2020, 1, 01).strftime("%Y-%m-%d"), datetime.date(2020, 12,
                                                                                                   31).strftime(
                "%Y-%m-%d")
        if start_date <= datetime.date(2017, 1, 1) and end_date >= datetime.date(2017, 12, 31):
            print "Y" + str(start_date.year)
            years_to_remove.append(2017)
            results["Y" + "2017"] = datetime.date(2017, 1, 01).strftime("%Y-%m-%d"), datetime.date(2017, 12,
                                                                                                   31).strftime(
                "%Y-%m-%d")

        if start_date <= datetime.date(2016, 1, 1) and end_date >= datetime.date(2016, 12, 31):
            print start_date.year
            years_to_remove.append(2016)
            results["Y" + "2016"] = datetime.date(2016, 1, 01).strftime("%Y-%m-%d"), datetime.date(2016, 12,
                                                                                                   31).strftime(
                "%Y-%m-%d")

        if start_date <= datetime.date(2015, 1, 1) and end_date >= datetime.date(2015, 12, 31):
            print start_date.year
            years_to_remove.append(2015)
            results["Y" + "2015"] = datetime.date(2015, 1, 01).strftime("%Y-%m-%d"), datetime.date(2015, 12,31).strftime("%Y-%m-%d")

        if start_date <= datetime.date(2014, 1, 1) and end_date >= datetime.date(2014, 12, 31):
            print start_date.year
            years_to_remove.append(2014)
            results["Y" + "2014"] = datetime.date(2014, 1, 01).strftime("%Y-%m-%d"), datetime.date(2014,12,31).strftime(
                "%Y-%m-%d")

        if start_date <= datetime.date(2021, 1, 1) and end_date >= datetime.date(2021, 12, 31):
            print start_date.year
            years_to_remove.append(2021)
            results["Y" + "2021"] = datetime.date(2021, 1, 01).strftime("%Y-%m-%d"), datetime.date(2021,12,31).strftime("%Y-%m-%d")
        return years_to_remove

    # Q1 = [1, 2, 3]
    # Q2 = [4, 5, 6]
    # Q3 = [7, 8, 9]
    # Q4 = [10, 11, 12]

    quarters_to_remove = []

    def check_quarter(start_date, end_date):
        global records, Q1, Q2, Q3, Q4
        print start_date, end_date
        quarters = []

        # if start_date.month <= 1 and start_date.day <= 1 and end_date.month >= 3 and end_date.day >= 31:
        #     print "Q1"
        #     quarters_to_remove.append(Q1)
        # if start_date.month <= 4 and start_date.day <= 1 and end_date.month >= 6 and end_date.day >= 30:
        #     print "Q2"
        #     quarters_to_remove.append(Q2)
        # if start_date.month <= 7 and start_date.day <= 1 and end_date.month >= 9 and end_date.day >= 30:
        #     print "Q3"
        #     quarters_to_remove.append(Q3)
        # if start_date.month <= 10 and start_date.day <= 1 and end_date.month >= 12 and end_date.day >= 31:
        #     print "Q4"
        #     quarters_to_remove.append(Q4)
        if end_date.year == start_date.year:
            if (start_date.month == 1 and start_date.day == 1) and (
                    end_date.month > 3 or (end_date.month == 3 and end_date.day == 31)):
                print "--------Q1"
                quarters.append(Q1)
                results["Q" + str(start_date.year) + "1"] = datetime.date(start_date.year, 01, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date.year, 03, 31).strftime("%Y-%m-%d")
            if (start_date.month < 4 or (start_date.month == 4 and start_date.day == 1)) and (
                    end_date.month > 6 or (end_date.month == 6 and end_date.day == 30)):
                print "///////////Q2"
                quarters.append(Q2)
                results["Q" + str(start_date.year) + "2"] = datetime.date(start_date.year, 04, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date.year, 06, 30).strftime("%Y-%m-%d")
            if (start_date.month < 7 or (start_date.month == 7 and start_date.day == 1)) and (
                    end_date.month > 9 or (end_date.month == 9 and end_date.day == 30)):
                print "Q3"
                results["Q" + str(start_date.year) + "3"] = datetime.date(start_date.year, 07, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date.year, 9, 30).strftime("%Y-%m-%d")
                quarters.append(Q3)
            if (start_date.month < 10 or (start_date.month == 10 and start_date.day == 1)) and (
                    end_date.month == 12 and end_date.day == 31):
                print "Q4"
                results["Q" + str(start_date.year) + "4"] = datetime.date(start_date.year, 10, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date.year, 12, 31).strftime("%Y-%m-%d")
                quarters.append(Q4)
            quarters_to_remove.append(quarters)

        elif end_date.year - start_date.year == 1:
            print "------------------"
            start_date_next1 = start_date
            end_date_next1 = datetime.date(start_date_next1.year, 12, 31)
            start_date_next2 = datetime.date(end_date.year, 1, 1)
            end_date_next2 = end_date
            if (start_date_next1.month == 1 and start_date_next1.day == 1) and (
                    end_date_next1.month > 3 or (end_date_next1.month == 3 and end_date_next1.day == 31)):
                print "Q1"
                quarters.append(Q1)
                results["Q" + str(start_date_next1.year) + "1"] = datetime.date(start_date_next1.year, 01, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next1.year, 03, 31).strftime("%Y-%m-%d")
            if (start_date_next1.month < 4 or (start_date_next1.month == 4 and start_date_next1.day == 1)) and (
                    end_date_next1.month > 6 or (end_date_next1.month == 6 and end_date_next1.day == 30)):
                print "Q2"
                quarters.append(Q2)
                results["Q" + str(start_date_next1.year) + "2"] = datetime.date(start_date_next1.year, 04, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next1.year, 06, 30).strftime("%Y-%m-%d")
            if (start_date_next1.month < 7 or (start_date_next1.month == 7 and start_date_next1.day == 1)) and (
                    end_date_next1.month > 9 or (end_date_next1.month == 9 and end_date_next1.day == 30)):
                print "Q3"
                quarters.append(Q3)
                results["Q" + str(start_date_next1.year) + "3"] = datetime.date(start_date_next1.year, 07, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next1.year, 9, 30).strftime("%Y-%m-%d")
                #             results.append("Q"+str(start_date_next1.year)+"3")
            if (start_date_next1.month < 10 or (start_date_next1.month == 10 and start_date_next1.day == 1)) and (
                    end_date_next1.month == 12 and end_date_next1.day == 31):
                print "Q4"
                quarters.append(Q4)
                results["Q" + str(start_date_next1.year) + "4"] = datetime.date(start_date_next1.year, 10, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next1.year, 12, 31).strftime("%Y-%m-%d")
            quarters_to_remove.append(quarters)
            quarters = []

            if (start_date_next2.month == 1 and start_date_next2.day == 1) and (
                    end_date_next2.month > 3 or (end_date_next2.month == 3 and end_date_next2.day == 31)):
                print "Q1"
                quarters.append(Q1)
                results["Q" + str(start_date_next2.year) + "1"] = datetime.date(start_date_next2.year, 01, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next2.year, 03, 31).strftime("%Y-%m-%d")
            if (start_date_next2.month < 4 or (start_date_next2.month == 4 and start_date_next2.day == 1)) and (
                    end_date_next2.month > 6 or (end_date_next2.month == 6 and end_date_next2.day == 30)):
                print "Q2"
                quarters.append(Q2)
                results["Q" + str(start_date_next2.year) + "2"] = datetime.date(start_date_next2.year, 04, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next2.year, 06, 30).strftime("%Y-%m-%d")
            if (start_date_next2.month < 7 or (start_date_next2.month == 7 and start_date_next2.day == 1)) and (
                    end_date_next2.month > 9 or (end_date_next2.month == 9 and end_date_next2.day == 30)):
                print "Q3"
                quarters.append(Q3)
                results["Q" + str(start_date_next2.year) + "3"] = datetime.date(start_date_next2.year, 07, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next2.year, 9, 30).strftime("%Y-%m-%d")
            if (start_date_next2.month < 10 or (start_date_next2.month == 10 and start_date_next2.day == 1)) and (
                    end_date_next2.month == 12 and end_date_next2.day == 31):
                print "Q4"
                quarters.append(Q4)
                results["Q" + str(start_date_next2.year) + "4"] = datetime.date(start_date_next2.year, 10, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next2.year, 12, 31).strftime("%Y-%m-%d")
            quarters_to_remove.append(quarters)
            quarters = []

        elif end_date.year - start_date.year == 2:
            start_date_next1 = start_date
            end_date_next1 = datetime.date(start_date_next1.year, 12, 31)
            start_date_next2 = datetime.date(start_date_next1.year + 1, 1, 1)
            end_date_next2 = datetime.date(start_date_next1.year + 1, 12, 31)
            start_date_next3 = datetime.date(end_date.year, 1, 1)
            end_date_next3 = end_date
            if (start_date_next1.month == 1 and start_date_next1.day == 1) and (
                    end_date_next1.month > 3 or (end_date_next1.month == 3 and end_date_next1.day == 31)):
                print "Q1"
                quarters.append(Q1)
                results["Q" + str(start_date_next1.year) + "1"] = datetime.date(start_date_next1.year, 01, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next1.year, 03, 31).strftime("%Y-%m-%d")
            if (start_date_next1.month < 4 or (start_date_next1.month == 4 and start_date_next1.day == 1)) and (
                    end_date_next1.month > 6 or (end_date_next1.month == 6 and end_date_next1.day == 30)):
                print "Q2"
                quarters.append(Q2)
                # print "Q" + str(start_date_next1.year) + "2"
                results["Q" + str(start_date_next1.year) + "2"] = datetime.date(start_date_next1.year, 04, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next1.year, 06, 30).strftime("%Y-%m-%d")
            if (start_date_next1.month < 7 or (start_date_next1.month == 7 and start_date_next1.day == 1)) and (
                    end_date_next1.month > 9 or (end_date_next1.month == 9 and end_date_next1.day == 30)):
                print "Q3"
                quarters.append(Q3)
                results["Q" + str(start_date_next1.year) + "3"] = datetime.date(start_date_next1.year, 07, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next1.year, 9, 30).strftime("%Y-%m-%d")
                #             results.append("Q"+str(start_date_next1.year)+"3")
            if (start_date_next1.month < 10 or (start_date_next1.month == 10 and start_date_next1.day == 1)) and (
                    end_date_next1.month == 12 and end_date_next1.day == 31):
                print "Q4"
                quarters.append(Q4)
                results["Q" + str(start_date_next1.year) + "4"] = datetime.date(start_date_next1.year, 10, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next1.year, 12, 31).strftime("%Y-%m-%d")
            quarters_to_remove.append(quarters)
            # print quarters_to_remove
            # print quarters
            quarters = []

            if (start_date_next2.month == 1 and start_date_next2.day == 1) and (
                    end_date_next2.month > 3 or (end_date.month == 3 and end_date.day == 31)):
                print "Q1"
                quarters.append(Q1)
                results["Q" + str(start_date_next2.year) + "1"] = datetime.date(start_date_next2.year, 01, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next2.year, 03, 31).strftime("%Y-%m-%d")
            if (start_date_next2.month < 4 or (start_date_next2.month == 4 and start_date_next2.day == 1)) and (
                    end_date_next2.month > 6 or (end_date_next2.month == 6 and end_date_next2.day == 30)):
                print "Q2"
                quarters.append(Q2)
                results["Q" + str(start_date_next2.year) + "2"] = datetime.date(start_date_next2.year, 04, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next2.year, 06, 30).strftime("%Y-%m-%d")
            if (start_date_next2.month < 7 or (start_date_next2.month == 7 and start_date_next2.day == 1)) and (
                    end_date_next2.month > 9 or (end_date_next2.month == 9 and end_date_next2.day == 30)):
                print "Q3"
                quarters.append(Q3)
                results["Q" + str(start_date_next2.year) + "3"] = datetime.date(start_date_next2.year, 07, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next2.year, 9, 30).strftime("%Y-%m-%d")
            if (start_date_next2.month < 10 or (start_date_next2.month == 10 and start_date_next2.day == 1)) and (
                    end_date_next2.month == 12 and end_date_next2.day == 31):
                print "Q4"
                quarters.append(Q4)
                results["Q" + str(start_date_next2.year) + "4"] = datetime.date(start_date_next2.year, 10, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next2.year, 12, 31).strftime("%Y-%m-%d")
            quarters_to_remove.append(quarters)
            #         quarters=[]
            # print quarters
            # print quarters_to_remove
            quarters = []
            if (start_date_next3.month == 1 and start_date_next3.day == 1) and (
                    end_date_next3.month > 3 or (end_date_next3.month == 3 and end_date_next3.day == 31)):
                print "Q1"
                quarters.append(Q1)
                results["Q" + str(start_date_next3.year) + "1"] = datetime.date(start_date_next3.year, 01, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next3.year, 03, 31).strftime("%Y-%m-%d")
            if (start_date_next3.month < 4 or (start_date_next3.month == 4 and start_date_next3.day == 1)) and (
                    end_date_next3.month > 6 or (end_date_next3.month == 6 and end_date_next3.day == 30)):
                print "Q2"
                quarters.append(Q2)
                results["Q" + str(start_date_next3.year) + "2"] = datetime.date(start_date_next3.year, 04, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next3.year, 06, 30).strftime("%Y-%m-%d")
            if (start_date_next3.month < 7 or (start_date_next3.month == 7 and start_date_next3.day == 1)) and (
                    end_date_next3.month > 9 or (end_date_next3.month == 9 and end_date.day == 30)):
                print "Q3"
                quarters.append(Q3)
                results["Q" + str(start_date_next3.year) + "3"] = datetime.date(start_date_next3.year, 07, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next3.year, 9, 30).strftime("%Y-%m-%d")
            if (start_date_next3.month < 10 or (start_date_next3.month == 10 and start_date_next3.day == 1)) and (
                    end_date_next3.month == 12 and end_date_next3.day == 31):
                print "Q4"
                quarters.append(Q4)
                results["Q" + str(start_date_next3.year) + "4"] = datetime.date(start_date_next3.year, 10, 01).strftime(
                    "%Y-%m-%d"), datetime.date(start_date_next3.year, 12, 31).strftime("%Y-%m-%d")
            quarters_to_remove.append(quarters)
            print quarters_to_remove

        return quarters_to_remove

    months_to_remove = []

    def check_month(start_date, end_date):
        months = []
        if start_date <= datetime.date(start_date.year, 1, 1) and end_date >= datetime.date(start_date.year, 1, 31):
            months.append(1)
            results["M" + str(start_date.year) + "1"] = datetime.date(start_date.year, 1, 01).strftime(
                "%Y-%m-%d"), datetime.date(start_date.year, 1, 31).strftime("%Y-%m-%d")
        if start_date <= datetime.date(start_date.year, 2, 1) and end_date >= datetime.date(start_date.year, 2, 30):
            months.append(2)
            results["M" + str(start_date.year) + "2"] = datetime.date(start_date.year, 2, 01).strftime(
                "%Y-%m-%d"), datetime.date(start_date.year, 1, 30).strftime("%Y-%m-%d")
        if start_date <= datetime.date(start_date.year, 3, 1) and end_date >= datetime.date(start_date.year, 3, 31):
            months.append(3)
            results["M" + str(start_date.year) + "3"] = datetime.date(start_date.year, 3, 01).strftime(
                "%Y-%m-%d"), datetime.date(start_date.year, 3, 31).strftime("%Y-%m-%d")
        if start_date <= datetime.date(start_date.year, 4, 1) and end_date >= datetime.date(start_date.year, 4, 30):
            months.append(4)
            results["M" + str(start_date.year) + "4"] = datetime.date(start_date.year, 4, 01).strftime(
                "%Y-%m-%d"), datetime.date(start_date.year, 4, 30).strftime("%Y-%m-%d")
        if start_date <= datetime.date(start_date.year, 5, 1) and end_date >= datetime.date(start_date.year, 5, 31):
            months.append(5)
            results["M" + str(start_date.year) + "5"] = datetime.date(start_date.year, 5, 01).strftime(
                "%Y-%m-%d"), datetime.date(start_date.year, 5, 31).strftime("%Y-%m-%d")
        if start_date <= datetime.date(start_date.year, 6, 1) and end_date >= datetime.date(start_date.year, 6, 30):
            months.append(6)
            results["M" + str(start_date.year) + "6"] = datetime.date(start_date.year, 6, 01).strftime(
                "%Y-%m-%d"), datetime.date(start_date.year, 6, 30).strftime("%Y-%m-%d")

        if start_date <= datetime.date(start_date.year, 7, 1) and end_date >= datetime.date(start_date.year, 7, 31):
            months.append(7)
            results["M" + str(start_date.year) + "7"] = datetime.date(start_date.year, 7, 01).strftime(
                "%Y-%m-%d"), datetime.date(start_date.year, 7, 30).strftime("%Y-%m-%d")
        if start_date <= datetime.date(start_date.year, 8, 1) and end_date >= datetime.date(start_date.year, 8, 31):
            months.append(8)
            results["M" + str(start_date.year) + "8"] = datetime.date(start_date.year, 8, 01).strftime(
                "%Y-%m-%d"), datetime.date(start_date.year, 8, 31).strftime("%Y-%m-%d")
        if start_date <= datetime.date(start_date.year, 9, 1) and end_date >= datetime.date(start_date.year, 9, 30):
            months.append(9)
            results["M" + str(start_date.year) + "9"] = datetime.date(start_date.year, 9, 01).strftime(
                "%Y-%m-%d"), datetime.date(start_date.year, 9, 30).strftime("%Y-%m-%d")

        if start_date <= datetime.date(start_date.year, 10, 1) and end_date >= datetime.date(start_date.year, 10, 31):
            months.append(10)
            results["M" + str(start_date.year) + "10"] = datetime.date(start_date.year, 10, 01).strftime(
                "%Y-%m-%d"), datetime.date(start_date.year, 10, 31).strftime("%Y-%m-%d")
        if start_date <= datetime.date(start_date.year, 11, 1) and end_date >= datetime.date(start_date.year, 11, 30):
            months.append(11)
            results["M" + str(start_date.year) + "11"] = datetime.date(start_date.year, 11, 01).strftime(
                "%Y-%m-%d"), datetime.date(start_date.year, 11, 30).strftime("%Y-%m-%d")
        if start_date <= datetime.date(start_date.year, 12, 1) and end_date >= datetime.date(start_date.year, 12, 31):
            months.append(12)
            results["M" + str(start_date.year) + "12"] = datetime.date(start_date.year, 12, 01).strftime(
                "%Y-%m-%d"), datetime.date(start_date.year, 12, 31).strftime("%Y-%m-%d")
        months_to_remove.append(months)
        print "joscj--------",months_to_remove
        return months_to_remove

    # w1_1 = datetime.date(start_date.year - 1, 12, 31)
    # w1_2 = w1_1 + datetime.timedelta(days=6)
    # week_dict = {"w1_1": w1_1, "w1_2": w1_2}
    # j = 0
    # for i in range(2, 53):
    #     week_dict["w" + str(i) + "_1"] = week_dict["w" + str(i - 1) + "_2"] + datetime.timedelta(days=1)
    #     week_dict["w" + str(i) + "_2"] = week_dict["w" + str(i) + "_1"] + datetime.timedelta(days=6)
    weeks=[]
    week_dates=[]
    def check_week(start_date, end_date):
        w1_1 = datetime.date(start_date.year - 1, 12, 31)
        w1_2 = w1_1 + datetime.timedelta(days=6)
        week_dict = {"w1_1": w1_1, "w1_2": w1_2}
        j = 0
        for i in range(2, 53):
            week_dict["w" + str(i) + "_1"] = week_dict["w" + str(i - 1) + "_2"] + datetime.timedelta(days=1)
            week_dict["w" + str(i) + "_2"] = week_dict["w" + str(i) + "_1"] + datetime.timedelta(days=6)

        print "week",start_date,end_date
        for i in range(1, 53):
            if start_date <= week_dict['w' + str(i) + '_1'] and end_date >= week_dict['w' + str(i) + '_2']:
                weeks.append(i)
                # print weeks
                week_dates.append(week_dict['w' + str(i) + '_1'])
                week_dates.append(week_dict['w' + str(i) + '_2'])
                results["W" + str(start_date.year) + str(i)] = week_dict['w' + str(i) + '_1'].strftime("%Y-%m-%d"), week_dict[
                                'w' + str(i) + '_2'].strftime("%Y-%m-%d")


    diff = end_date - start_date
    if diff > datetime.timedelta(365):
        print "in check_year"
        print start_date, end_date
        check_year(start_date, end_date)
        print "years_to_remove: ", years_to_remove
    else:
        start_date1 = start_date
        end_date1 = end_date
        start_date2 = 0
        end_date2 = 0

    if len(years_to_remove) != 0:
        start_date1 = start_date
        end_date1 = datetime.date(years_to_remove[0], 1, 1) - datetime.timedelta(days=1)
        start_date2 = datetime.date(years_to_remove[-1], 12, 31) + datetime.timedelta(days=1)
        end_date2 = end_date
        if end_date1 < start_date1:
            start_date1 = 0
            end_date1 = 0
        if end_date2 < start_date2:
            start_date2 = 0
            end_date2 = 0

        print "after years to remove",start_date1, end_date1, start_date2, end_date2

    try:
        start_date1, end_date1
    except:
        start_date1 = start_date
        end_date1 = end_date
        start_date2 = 0
        end_date2 = 0

    if start_date1 !=0 and end_date1 !=0:
        num_months1 = [dt.strftime("%m") for dt in rrule(MONTHLY, dtstart=start_date1, until=end_date1)]
    if start_date2 !=0 and end_date2 != 0:
        num_months2 = [dt.strftime("%m") for dt in rrule(MONTHLY, dtstart=start_date2, until=end_date2)]

    if len(num_months1) >= 3:
        check_quarter(start_date1, end_date1)
        print "1",quarters_to_remove
    if start_date2 != 0 and end_date2 != 0:
        num_months2 = [dt.strftime("%m") for dt in rrule(MONTHLY, dtstart=start_date2, until=end_date2)]
    try:
        if len(num_months2) >= 3:
            check_quarter(start_date2, end_date2)
            print "2", quarters_to_remove
    except:
        pass
    # print results
    try:
        if len(quarters_to_remove) != 0:
            # if end_date1.year - start_date1.year == 1:
            #     print
            start_date11 = start_date1
            try:
                end_date11 = datetime.date(start_date11.year, quarters_to_remove[0][0][0] - 1, 31)
            except:
                end_date11 = datetime.date(start_date11.year, quarters_to_remove[0][0][0] - 1, 30)
            # datetime.date(quarters_to_remove[0][0],1,1)-datetime.timedelta(days=1)
            try:
                start_date22 = datetime.date(start_date11.year, quarters_to_remove[0][-1][-1] + 1, 1)
            except:
                start_date22 = datetime.date(start_date11.year + 1, 1, 1)
            if end_date1.year - start_date1.year ==0:
                end_date22 = end_date1
            else:
                end_date22 = datetime.date(start_date11.year, 12, 31)
            if end_date1.year-start_date1.year ==1:

                start_date111 = datetime.date(end_date1.year, 1, 1)
                try:
                    if quarters_to_remove[1][0][0] == 1:
                        start_date111 = 0
                        end_date111 = 0
                    else:
                        end_date111 = datetime.date(end_date1.year, quarters_to_remove[1][0][0] - 1, 31)
                except:
                    end_date111 = datetime.date(end_date1.year, quarters_to_remove[1][0][0] - 1, 30)
                try:
                    start_date222 = datetime.date(end_date1.year, quarters_to_remove[1][-1][-1] + 1, 1)
                except:
                    start_date222 = datetime.date(end_date1.year + 1, 1, 1)
                end_date222 = end_date1
            # else:


            if end_date11 < start_date11:
                start_date11 = 0
                end_date11 = 0
            if end_date22 < start_date22:
                start_date22 = 0
                end_date22 = 0
            try:
                if end_date111 < start_date111:
                    start_date111 = 0
                    end_date111 = 0
                if end_date222 < start_date222:
                    start_date222 = 0
                    end_date222 = 0
            # try:
            #     end_date111
            except:
                start_date111 = 0
                end_date111 = 0
                start_date222 = 0
                end_date222 = 0
        else:
            start_date11=start_date
            end_date11=end_date
    except:
        start_date11 = start_date
        end_date11 = end_date

    try:
        if start_date2 != 0 and end_date2 != 0:
            start_date33 = start_date2
            try:
                end_date33 = datetime.date(start_date2.year, quarters_to_remove[-1][0] - 1, 31)
            except:
                end_date33 = datetime.date(start_date2.year - 1, 12, 31)
            # datetime.date(quarters_to_remove[0][0],1,1)-datetime.timedelta(days=1)
            try:
                try:
                    start_date44 = datetime.date(start_date2.year, quarters_to_remove[-1][-1][-1] + 1, 1)
                except:
                    start_date44 = datetime.date(start_date2.year, quarters_to_remove[-1][-1] + 1, 1)
            except:
                start_date44 = datetime.date(start_date2.year + 1, 1, 1)
            end_date44 = end_date2
            if end_date33 < start_date33:
                start_date33 = 0
                end_date33 = 0
            if end_date44 < start_date44:
                start_date44 = 0
                end_date44 = 0
    except:
        pass

    print "================="
    print start_date,end_date
    print "years_to_remove list",years_to_remove
    print start_date1,end_date1,start_date2,end_date2
    print "quarters_to_remove list",quarters_to_remove
    try:
        print "after quarters removing1",start_date11,end_date11,start_date22,end_date22,start_date111,end_date111,start_date222,end_date222
    except:
        pass
    try:
        print "after quarters removing2",start_date33,end_date33,start_date44,end_date44
    except:
        pass

    try:
        print end_date11
        if end_date11 - start_date11 >= datetime.timedelta(30):
            check_month(start_date11, end_date11)
            print "1", months_to_remove
        else:
            months_to_remove.append([])
    except:
        months_to_remove.append([])
        pass

    try:
        print end_date22
        if end_date22 - start_date22 >= datetime.timedelta(30):
            check_month(start_date22, end_date22)
            print "2", months_to_remove
        else:
            months_to_remove.append([])
    except:
        months_to_remove.append([])
        pass

    try:
        print end_date111
        if end_date111 - start_date111 >= datetime.timedelta(30):
            check_month(start_date111, end_date111)
            print "3", months_to_remove
        else:
            months_to_remove.append([])
    except:
        print "3"
        months_to_remove.append([])
        pass

    try:
        print end_date222
        if end_date222 - start_date222 >= datetime.timedelta(30):
            check_month(start_date222, end_date222)
            print "4", months_to_remove
        else:
            months_to_remove.append([])
    except:
        months_to_remove.append([])
        pass

    try:
        print end_date33
        if end_date33 - start_date33 >= datetime.timedelta(30):
            check_month(start_date33, end_date33)
            print "5", months_to_remove
        else:
            months_to_remove.append([])
    except:
        months_to_remove.append([])
        pass
    try:
        print end_date44
        if end_date44 - start_date44 >= datetime.timedelta(30):
            check_month(start_date44, end_date44)
            print "6", months_to_remove
        else:
            months_to_remove.append([])
    except:
        months_to_remove.append([])
        pass

    print months_to_remove

    if len(months_to_remove[0]) != 0:
        start_date_11 = start_date11
        try:
            end_date_11 = datetime.date(start_date_11.year, months_to_remove[0][0] - 1, 31)
        except:
            end_date_11 = datetime.date(start_date_11.year, months_to_remove[0][0] - 1, 30)
        start_date_22 = datetime.date(start_date_11.year, months_to_remove[0][-1] + 1, 1)
        end_date_22 = end_date11
        if end_date_11 < start_date_11:
            end_date_11 = 0
            start_date_11 = 0
        if end_date_22 < start_date_22:
            end_date_22 = 0
            start_date_22 = 0
    print "months to remove list", months_to_remove
    if len(months_to_remove[1]) != 0:
        start_date_33 = start_date22
        try:
            end_date_33 = datetime.date(start_date_33.year, months_to_remove[1][0] - 1, 31)
        except:
            end_date_33 = datetime.date(start_date_33.year, months_to_remove[1][0] - 1, 30)
        start_date_44 = datetime.date(start_date_33.year, months_to_remove[1][-1] + 1, 1)
        end_date_44 = end_date22
        if end_date_33 < start_date_33:
            end_date_33 = 0
            start_date_33 = 0
        if end_date_44 < start_date_44:
            end_date_44 = 0
            start_date_44 = 0

    if len(months_to_remove[2]) != 0:
        start_date_55 = start_date111
        try:
            end_date_55 = datetime.date(start_date_55.year, months_to_remove[2][0] - 1, 31)
        except:
            end_date_55 = datetime.date(start_date_55.year, months_to_remove[2][0] - 1, 30)
        start_date_66 = datetime.date(start_date_55.year, months_to_remove[2][-1] + 1, 1)
        end_date_66 = end_date111
        if end_date_55 < start_date_55:
            end_date_55 = 0
            start_date_55 = 0
        if end_date_66 < start_date_66:
            end_date_66 = 0
            start_date_66 = 0

    if len(months_to_remove[3]) != 0:
        print start_date222,end_date222
        start_date_77 = start_date222
        try:
            end_date_77 = datetime.date(start_date_77.year, months_to_remove[3][0] - 1, 31)
        except:
            end_date_77 = datetime.date(start_date_77.year, months_to_remove[3][0] - 1, 30)
        start_date_88 = datetime.date(start_date_77.year, months_to_remove[3][-1] + 1, 1)
        end_date_88 = end_date222
        print start_date_77, end_date_77,start_date_88,end_date_88
        if end_date_77 < start_date_77:
            end_date_77 = 0
            start_date_77 = 0
        if end_date_88 < start_date_88:
            print "True"
            end_date_88 = 0
            start_date_88 = 0

    if len(months_to_remove[4]) != 0:
        start_date_99 = start_date33
        try:
            end_date_99 = datetime.date(start_date_99.year, months_to_remove[4][0] - 1, 31)
        except:
            end_date_99 = datetime.date(start_date_99.year, months_to_remove[4][0] - 1, 30)
        start_date_10 = datetime.date(start_date_99.year, months_to_remove[4][-1] + 1, 1)
        end_date_10 = end_date33
        if end_date_99 < start_date_99:
            end_date_99 = 0
            start_date_99 = 0
        if end_date_10 < start_date_10:
            end_date_10 = 0
            start_date_10 = 0
    print " months to remove list ", months_to_remove
    if len(months_to_remove[5]) != 0:
        start_date_111 = start_date44
        try:
            end_date_111 = datetime.date(start_date_111.year, months_to_remove[5][0] - 1, 31)
        except:
            end_date_111 = datetime.date(start_date_111c.year, months_to_remove[5][0] - 1, 30)
        start_date_12 = datetime.date(start_date_111.year, months_to_remove[5][-1] + 1, 1)
        end_date_12 = end_date44
        if end_date_111 < start_date_111:
            end_date_111 = 0
            start_date_111 = 0
        if end_date_12 < start_date_12:
            end_date_12 = 0
            start_date_12 = 0
    weeks_to_remove=[]
    week_dates_to_remove=[]
    try:
        start_date_11,end_date_11
    except:
        start_date_11=start_date11
        end_date_11 = end_date11
    try:
        start_date_22,end_date_22
    except:
        start_date_22=0
        end_date_22=0
    try:
        start_date_33,end_date_33
    except:
        start_date_33=0
        end_date_33=0
        start_date_44=0
        end_date_44=0
    try:
        start_date_55,end_date_55
    except:
        start_date_55=0
        end_date_55=0
    try:
        start_date_66, end_date_66
    except:
        start_date_66 = 0
        end_date_66 = 0
    try:
        start_date_77,end_date_77
    except:
        start_date_77=0
        end_date_77=0
    try:
        start_date_88, end_date_88
    except:
        start_date_88 = 0
        end_date_88 = 0
    try:
        start_date_99,end_date_99
    except:
        start_date_99=0
        end_date_99=0
    try:
        start_date_10, end_date_10
    except:
        start_date_10 = 0
        end_date_10 = 0
    try:
        start_date_111,end_date_111
    except:
        start_date_111=0
        end_date_111=0
    try:
        start_date_12, end_date_12
    except:
        start_date_12 = 0
        end_date_12 = 0

    print "after months to remove", start_date_11,end_date_11,start_date_22,end_date_22
    print start_date_33,end_date_33,start_date_44,end_date_44
    print start_date_55, end_date_55, start_date_66, end_date_66
    print start_date_77, end_date_77, start_date_88, end_date_88
    print start_date_99, end_date_99, start_date_10, end_date_10
    print start_date_111, end_date_111, start_date_12, end_date_12
    # print start_date_33, end_date_33, start_date_44, end_date_44
    print start_date_11,end_date_11
    print end_date_11 - start_date_11
    try:
        if end_date_11 - start_date_11 >= datetime.timedelta(6):
            print start_date_11,end_date_11
            check_week(start_date_11, end_date_11)
            print weeks
            # weeks_to_remove.append(weeks)
        else:
            print "true"
            pass
    except:
        print "false"
        pass
    weeks_to_remove.append(weeks)
    week_dates_to_remove.append(week_dates)
    weeks = []
    week_dates=[]
    try:
        if end_date_22 - start_date_22 >= datetime.timedelta(6):
            check_week(start_date_22, end_date_22)
            print weeks
            # weeks_to_remove.append(weeks)
        else:
            pass
    except:
        pass
    weeks_to_remove.append(weeks)
    week_dates_to_remove.append(week_dates)
    weeks = []
    week_dates = []
    try:
        if end_date_33 - start_date_33 >= datetime.timedelta(6):
            check_week(start_date_33, end_date_33)
            print weeks
            # weeks_to_remove.append(weeks)
        else:
            pass
    except:
        pass
    weeks_to_remove.append(weeks)
    week_dates_to_remove.append(week_dates)
    weeks = []
    week_dates = []
    try:
        if end_date_44 - start_date_44 >= datetime.timedelta(6):
            check_week(start_date_44, end_date_44)
            print weeks
            # weeks_to_remove.append(weeks)
        else:
            pass
    except:
        pass
    weeks_to_remove.append(weeks)
    week_dates_to_remove.append(week_dates)
    weeks = []
    week_dates = []
    try:
        if end_date_55 - start_date_55 >= datetime.timedelta(6):
            check_week(start_date_55, end_date_55)
            print weeks
        else:
            pass
    except:
        pass
    weeks_to_remove.append(weeks)
    week_dates_to_remove.append(week_dates)
    weeks = []
    week_dates = []
    try:
        # print "--"
        if end_date_66 - start_date_66 >= datetime.timedelta(6):
            check_week(start_date_66, end_date_66)
            print weeks
        else:
            pass
    except:
        pass
    weeks_to_remove.append(weeks)
    week_dates_to_remove.append(week_dates)
    weeks = []
    week_dates = []
    try:
        if end_date_77 - start_date_77 >= datetime.timedelta(6):
            check_week(start_date_77, end_date_77)
            print weeks
        else:
            pass
    except:
        pass
    weeks_to_remove.append(weeks)
    week_dates_to_remove.append(week_dates)
    weeks = []
    week_dates = []
    try:
        print "--"
        if end_date_88 - start_date_88 >= datetime.timedelta(6):
            check_week(start_date_88, end_date_88)
            print weeks
        else:
            pass
    except:
        pass
    weeks_to_remove.append(weeks)
    week_dates_to_remove.append(week_dates)
    weeks = []
    week_dates = []
    try:
        if start_date_99 != 0:
            if end_date_99 - start_date_99 >= datetime.timedelta(6):
                check_week(start_date_99, end_date_99)
                print weeks
    except:
        pass
    weeks_to_remove.append(weeks)
    week_dates_to_remove.append(week_dates)
    weeks = []
    week_dates = []
    try:
        if start_date_10 != 0:
            print "--"
            if end_date_10 - start_date_10 >= datetime.timedelta(6):
                check_week(start_date_10, end_date_10)
                print weeks
    except:
        pass
    weeks_to_remove.append(weeks)
    week_dates_to_remove.append(week_dates)
    weeks = []
    week_dates = []
    try:
        if start_date_111 != 0:
            if end_date_111 - start_date_111 >= datetime.timedelta(6):
                check_week(start_date_111, end_date_111)
                print weeks
    except:
        pass
    weeks_to_remove.append(weeks)
    week_dates_to_remove.append(week_dates)
    weeks = []
    week_dates = []
    try:
        if start_date_12 != 0:
            print "--"
            if end_date_12 - start_date_12 >= datetime.timedelta(6):
                check_week(start_date_12, end_date_12)
                print weeks
    except:
        pass
    weeks_to_remove.append(weeks)
    week_dates_to_remove.append(week_dates)
    weeks = []
    week_dates = []

    print "weeks to remove list",weeks_to_remove
    print "weeks dates to remove list", week_dates_to_remove

    if len(weeks_to_remove[0]) != 0:
        print "--pp"
        st11 = start_date_11
        en11 = week_dates_to_remove[0][0] - datetime.timedelta(days=1)
        st22 = week_dates_to_remove[0][-1] + datetime.timedelta(days=1)
        en22 = end_date_11
    else:
        st11 = start_date_11
        en11= end_date_11
    if len(weeks_to_remove[1]) != 0:
        st33 = start_date_22
        en33 = week_dates_to_remove[1][0] - datetime.timedelta(days=1)
        st44 = week_dates_to_remove[1][-1] + datetime.timedelta(days=1)
        en44 = end_date_22

    if len(weeks_to_remove[2]) != 0:
        st55 = start_date_33
        en55 = week_dates_to_remove[2][0] - datetime.timedelta(days=1)
        st66 = week_dates_to_remove[2][-1] + datetime.timedelta(days=1)
        en66 = end_date_33

    if len(weeks_to_remove[3]) != 0:
        st77 = start_date_44
        en77 = week_dates_to_remove[3][0] - datetime.timedelta(days=1)
        st88 = week_dates_to_remove[3][-1] + datetime.timedelta(days=1)
        en88 = end_date_44
        print st77,en77,st88,en88
    if len(weeks_to_remove[4]) != 0:
        st99 = start_date_55
        en99 = week_dates_to_remove[4][0] - datetime.timedelta(days=1)
        st10 = week_dates_to_remove[4][-1] + datetime.timedelta(days=1)
        en10 = end_date_55
    if len(weeks_to_remove[5]) != 0:
        st111 = start_date_66
        en111 = week_dates_to_remove[5][0] - datetime.timedelta(days=1)
        st12 = week_dates_to_remove[5][-1] + datetime.timedelta(days=1)
        en12 = end_date_66
    if len(weeks_to_remove[6]) != 0:
        st13 = start_date_77
        en13 = week_dates_to_remove[6][0] - datetime.timedelta(days=1)
        st14 = week_dates_to_remove[6][-1] + datetime.timedelta(days=1)
        en14 = end_date_77
    if len(weeks_to_remove[7]) != 0:
        st15 = start_date_88
        en15 = week_dates_to_remove[7][0] - datetime.timedelta(days=1)
        st16 = week_dates_to_remove[7][-1] + datetime.timedelta(days=1)
        en16 = end_date_88
    if len(weeks_to_remove[8]) != 0:
        st17 = start_date_99
        en17 = week_dates_to_remove[8][0] - datetime.timedelta(days=1)
        st18 = week_dates_to_remove[8][-1] + datetime.timedelta(days=1)
        en18 = end_date_99
    if len(weeks_to_remove[9]) != 0:
        st19 = start_date_10
        en19 = week_dates_to_remove[9][0] - datetime.timedelta(days=1)
        st20 = week_dates_to_remove[9][-1] + datetime.timedelta(days=1)
        en20 = end_date_10
    if len(weeks_to_remove[10]) != 0:
        st21 = start_date_111
        en21 = week_dates_to_remove[10][0] - datetime.timedelta(days=1)
        st22 = week_dates_to_remove[10][-1] + datetime.timedelta(days=1)
        en22 = end_date_111
    if len(weeks_to_remove[11]) != 0:
        st23 = start_date_12
        en23 = week_dates_to_remove[11][0] - datetime.timedelta(days=1)
        st24 = week_dates_to_remove[11][-1] + datetime.timedelta(days=1)
        en24 = end_date_12


    days_to_remove = []
    try:
        print st11,en11
        for i in range((en11 - st11).days+1):
            days_to_remove.append(st11 + datetime.timedelta(days=i))
    except:
        pass
    try:
        print st22, en22
        for i in range((en22 - st22).days+1):
            days_to_remove.append(st22 + datetime.timedelta(days=i))
    except:
        pass
    try:
        print st33,en33,st44,en44
        for i in range((en33 - st33).days+1):
            days_to_remove.append(st33 + datetime.timedelta(days=i))
        for i in range((en44 - st44).days+1):
            days_to_remove.append(st44 + datetime.timedelta(days=i))

    except:
        pass
    try:
        print st55,en55,st66,en66
        for i in range((en55 - st55).days+1):
            days_to_remove.append(st55 + datetime.timedelta(days=i))
        for i in range((en66 - st66).days+1):
            days_to_remove.append(st66 + datetime.timedelta(days=i))

    except:
        pass
    try:
        print st77,en77,st88,en88
        for i in range((en77 - st77).days+1):
            days_to_remove.append(st77 + datetime.timedelta(days=i))
        for i in range((en88 - st88).days+1):
            days_to_remove.append(st88 + datetime.timedelta(days=i))

    except:
        pass
    try:
        print st99,en99,st10,en10
        for i in range((en99 - st99).days+1):
            days_to_remove.append(st99 + datetime.timedelta(days=i))
        for i in range((en10 - st10).days+1):
            days_to_remove.append(st10 + datetime.timedelta(days=i))
    except:
        pass
    try:
        print st111,en111,st12,en12
        for i in range((en111 - st111).days+1):
            days_to_remove.append(st111 + datetime.timedelta(days=i))
        for i in range((en12 - st12).days+1):
            days_to_remove.append(st12 + datetime.timedelta(days=i))
    except:
        pass
    try:
        print st13,en13,st14,en14
        for i in range((en13 - st13).days+1):
            days_to_remove.append(st13 + datetime.timedelta(days=i))
        for i in range((en14 - st14).days+1):
            days_to_remove.append(st14 + datetime.timedelta(days=i))
    except:
        pass
    try:
        print st15,en15,st15,en15
        for i in range((en15 - st15).days+1):
            days_to_remove.append(st15 + datetime.timedelta(days=i))
        for i in range((en16 - st16).days+1):
            days_to_remove.append(st16 + datetime.timedelta(days=i))
    except:
        pass
    try:
        # print st33,en33,st44,en44
        for i in range((en17 - st17).days+1):
            days_to_remove.append(st17 + datetime.timedelta(days=i))
        for i in range((en18 - st18).days+1):
            days_to_remove.append(st18 + datetime.timedelta(days=i))
    except:
        pass
    try:
        # print st33,en33,st44,en44
        for i in range((en19 - st19).days+1):
            days_to_remove.append(st19 + datetime.timedelta(days=i))
        for i in range((en20 - st20).days+1):
            days_to_remove.append(st20 + datetime.timedelta(days=i))
    except:
        pass
    try:
        # print st33,en33,st44,en44
        for i in range((en21 - st21).days+1):
            days_to_remove.append(st21 + datetime.timedelta(days=i))
        for i in range((en22 - st22).days+1):
            days_to_remove.append(st22 + datetime.timedelta(days=i))
    except:
        pass
    try:
        # print st33,en33,st44,en44
        for i in range((en23 - st23).days+1):
            days_to_remove.append(st23 + datetime.timedelta(days=i))
        for i in range((en24 - st24).days+1):
            days_to_remove.append(st24 + datetime.timedelta(days=i))
    except:
        pass
    days_to_remove = np.unique(days_to_remove)
    print days_to_remove
    for i in range(len(days_to_remove)):
        results["D"+str(i)+""]=days_to_remove[i].strftime("%Y-%m-%d")

    # if start_date == end_date:
    #     results["D"] = start_date.strftime("%Y-%m-%d")
    # print days_to_remove
    print results
    print time.time()-st
        # start_date = datetime.date(2018, 3, 1)
    # end_date = datetime.date(2018, 12, 25)


if __name__=='__main__':
    quarter_logic(datetime.date(2018, 4, 17),datetime.date(2018,9,26))
