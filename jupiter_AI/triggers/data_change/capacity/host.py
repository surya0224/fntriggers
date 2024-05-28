"""
old_doc = {
	airline,
	origin,
	destination,
	compartment,
	month,
	year,
	capacity
}
new_doc = {
	airline,
	origin,
	destination,
	compartment,
	month,
	year,
	capacity
}
name = 'competitor_airline_capacity_percentage_change'
"""
from jupiter_AI.triggers.data_change.MainClass import DataChange
from jupiter_AI.triggers.common import get_start_end_dates
import pandas as pd
from jupiter_AI.network_level_params import JUPITER_DB, SYSTEM_DATE, Host_Airline_Code
from jupiter_AI import client, JUPITER_LOGGER
from jupiter_AI.logutils import measure
import jupiter_AI.common.ClassErrorObject as error_class
import inspect
import datetime

db = client[JUPITER_DB]


@measure(JUPITER_LOGGER)
def get_arg_lists(frame):
    """
    function used to get the list of arguments of the function
    where it is called
    """
    args, _, _, values = inspect.getargvalues(frame)
    argument_name_list = []
    argument_value_list = []
    for k in args:
        argument_name_list.append(k)
        argument_value_list.append(values[k])
    return argument_name_list, argument_value_list


class HostCapacityChange(DataChange):
    """
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, name, old_database_doc, new_database_doc):
        print name
        print old_database_doc
        print new_database_doc
        DataChange.__init__(self, name, old_database_doc, new_database_doc)
        self.triggering_event = dict(
            collection='JUP_DB_Host_OD_Capacity',
            field='capacity',
            action='change'
        )

    @measure(JUPITER_LOGGER)
    def build_triggering_data(self):
        """
        :return:
        """
        # dep_date_start, dep_date_end = get_start_end_dates(month=self.old_doc_data['month'],
        #                                                    year=self.old_doc_data['year'])
        # self.triggering_data = dict(dep_date_start=dep_date_start,
        #                             dep_date_end=dep_date_end)

        self.triggering_data = dict(
            dep_date_start=self.old_doc_data['dep_date'],
            dep_date_end=self.old_doc_data['dep_date']
        )

    @measure(JUPITER_LOGGER)
    def check_trigger(self):
       return True


@measure(JUPITER_LOGGER)
def get_host_od_cap(coll_name):
    cap_crsr = list(db.coll_name.find({
        "month": {"$gte": SYSTEM_DATE[5:7]}
    }, {
        "od": 1,
        "_id": 0,
        "y_cap": 1,
        "j_cap": 1,
        "dep_date": 1
    }))

    cap_df = pd.DataFrame(cap_crsr)
    return cap_df


@measure(JUPITER_LOGGER)
def generate_host_od_capacity_change_triggers():
    od_cap_triggers = []
    name = 'host_airline_capacity_percentage_change'
    trigger_crsr = list(db.JUP_DB_Trigger_Types.find({"desc": name}, {"lower_threshhold": 1,
                                                                      "upper_threshhold": 1}))

    lower_thresh = trigger_crsr[0]['lower_threshhold']
    upper_thresh = trigger_crsr[0]['upper_threshhold']

    # getting host_od_cap for last snap
    host_od_cap_lsnap = get_host_od_cap(coll_name="JUP_DB_Host_OD_Capacity_1")
    y_cap = host_od_cap_lsnap[['od', 'dep_date', 'y_cap']]
    y_cap['compartment'] = "Y"
    y_cap = y_cap.rename(columns={"y_cap": "capacity"})
    j_cap = host_od_cap_lsnap[['od', 'dep_date', 'j_cap']]
    j_cap['compartment'] = "J"
    j_cap = j_cap.rename(columns={"j_cap": "capacity"})
    del host_od_cap_lsnap
    host_od_cap_lsnap = pd.concat([y_cap, j_cap])

    # getting host_od_cap for current snap
    host_od_cap_csnap = get_host_od_cap(coll_name="JUP_DB_Host_OD_Capacity")
    y_cap = host_od_cap_csnap[['od', 'dep_date', 'y_cap']]
    y_cap['compartment'] = "Y"
    y_cap = y_cap.rename(columns={"y_cap": "capacity"})
    j_cap = host_od_cap_csnap[['od', 'dep_date', 'j_cap']]
    j_cap['compartment'] = "J"
    j_cap = j_cap.rename(columns={"j_cap": "capacity"})
    del host_od_cap_csnap
    host_od_cap_csnap = pd.concat([y_cap, j_cap])

    del y_cap
    del j_cap

    host_od_cap_csnap = host_od_cap_csnap.merge(host_od_cap_lsnap,
                                                on=['od', 'dep_date', 'compartment'],
                                                how='left',
                                                suffixes=("", "_1"))
    del host_od_cap_lsnap

    host_od_cap_csnap.dropna(inplace=True)

    host_od_cap_csnap['pct_change'] = (host_od_cap_csnap['capacity'] - host_od_cap_csnap['capacity_1']) * 100 / \
                                      host_od_cap_csnap['capacity_1']
    host_od_cap_csnap['trigger_occurance'] = 0
    host_od_cap_csnap.loc[(host_od_cap_csnap['pct_change'] > upper_thresh) &
                          (host_od_cap_csnap['pct_change'] < lower_thresh), 'trigger_occurance'] = 1
    host_od_cap_csnap = host_od_cap_csnap[host_od_cap_csnap['trigger_occurance'] == 1]
    for idx, row in host_od_cap_csnap.iterrows():
        old_database_doc = dict(
            pos=row['od'][0:3],
            origin=row['od'][0:3],
            destination=row['od'][3:],
            compartment=row['compartment'],
            airline=Host_Airline_Code,
            dep_date=row['dep_date'],
            capacity=row['capacity_1']
        )
        new_database_doc = dict(
            pos=row['od'][0:3],
            origin=row['od'][0:3],
            destination=row['od'][3:],
            compartment=row['compartment'],
            airline=Host_Airline_Code,
            dep_date=row['dep_date'],
            capacity=row['capacity']
        )
        obj = HostCapacityChange(name=name,
                                 old_database_doc=old_database_doc,
                                 new_database_doc=new_database_doc)
        _id = obj.do_analysis()
        od_cap_triggers.append(_id)
        return od_cap_triggers


if __name__=='__main__':
    name = 'host_airline_capacity_percentage_change'
    old_database_doc = dict(
        pos=None,
        origin='DXB',
        destination='DOH',
        compartment='Y',
        airline='FZ',
        month=2,
        year=2017,
        capacity=15
    )
    new_database_doc = dict(
        pos=None,
        origin='DXB',
        destination='DOH',
        compartment='Y',
        airline='FZ',
        month=2,
        year=2017,
        capacity=30
    )

    obj = HostCapacityChange(name=name,
                             old_database_doc=old_database_doc,
                             new_database_doc=new_database_doc)
    obj.do_analysis()