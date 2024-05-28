from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure

import datetime

Q1 = [1, 2, 3]
Q2 = [4, 5, 6]
Q3 = [7, 8, 9]
Q4 = [10, 11, 12]


@measure(JUPITER_LOGGER)
def quarter_logic(start_date,end_date):
    import datetime
    import time
    import pandas as pd
    import numpy as np
    from dateutil.rrule import rrule, MONTHLY

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


    @measure(JUPITER_LOGGER)
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


    @measure(JUPITER_LOGGER)
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


    @measure(JUPITER_LOGGER)
    def check_month(start_date, end_date):
        months = []
        if start_date <= datetime.date(start_date.year, 1, 1) and end_date >= datetime.date(start_date.year, 1, 31):
            months.append(1)
        if start_date <= datetime.date(start_date.year, 2, 1) and end_date >= datetime.date(start_date.year, 2, 30):
            months.append(2)
        if start_date <= datetime.date(start_date.year, 3, 1) and end_date >= datetime.date(start_date.year, 3, 31):
            months.append(3)
        if start_date <= datetime.date(start_date.year, 4, 1) and end_date >= datetime.date(start_date.year, 4, 30):
            months.append(4)

        if start_date <= datetime.date(start_date.year, 5, 1) and end_date >= datetime.date(start_date.year, 5, 31):
            months.append(5)

        if start_date <= datetime.date(start_date.year, 6, 1) and end_date >= datetime.date(start_date.year, 6, 30):
            months.append(6)

        if start_date <= datetime.date(start_date.year, 7, 1) and end_date >= datetime.date(start_date.year, 7, 31):
            months.append(7)

        if start_date <= datetime.date(start_date.year, 8, 1) and end_date >= datetime.date(start_date.year, 8, 31):
            months.append(8)

        if start_date <= datetime.date(start_date.year, 9, 1) and end_date >= datetime.date(start_date.year, 9, 30):
            months.append(9)

        if start_date <= datetime.date(start_date.year, 10, 1) and end_date >= datetime.date(start_date.year, 10, 31):
            months.append(10)

        if start_date <= datetime.date(start_date.year, 11, 1) and end_date >= datetime.date(start_date.year, 11, 30):
            months.append(11)

        if start_date <= datetime.date(start_date.year, 12, 1) and end_date >= datetime.date(start_date.year, 12, 31):
            months.append(12)
        months_to_remove.append(months)
        return months_to_remove


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

        print start_date1, end_date1, start_date2, end_date2

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
    print results



    # start_date = datetime.date(2018, 3, 1)
    # end_date = datetime.date(2018, 12, 25)
quarter_logic(datetime.date(2018, 4, 26),datetime.date(2018, 6, 26))