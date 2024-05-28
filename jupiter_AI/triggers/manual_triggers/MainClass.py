"""
Manual Trigger -
"""
import json
import time
# print time.time()
from jupiter_AI.network_level_params import SYSTEM_DATE, JUPITER_LOGGER
from jupiter_AI import mongo_client, client, JUPITER_DB
from jupiter_AI.logutils import measure
from jupiter_AI.triggers.MainClass import Trigger
from jupiter_AI.triggers.listener_manual_trigger import analyze


class ManualTrigger(Trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, pos, origin, destination, compartment, dep_date_start, dep_date_end, reason, work_package_name, flag):
        super(ManualTrigger, self).__init__()
        self.old_doc_data = dict(
            pos=pos,
            origin=origin,
            destination=destination,
            compartment=compartment
        )
        self.new_doc_data = dict(
            pos=pos,
            origin=origin,
            destination=destination,
            compartment=compartment
        )
        self.triggering_data = dict(
            dep_date_start=dep_date_start,
            dep_date_end=dep_date_end
        )
        self.old_doc_data['reason'] = reason
        self.new_doc_data['reason'] = reason
        self.old_doc_data['work_package_name'] = work_package_name
        self.new_doc_data['work_package_name'] = work_package_name
        self.trigger_date = SYSTEM_DATE
        self.flag = flag
        # self.trigger_time =

    @measure(JUPITER_LOGGER)
    def check_trigger(self):
        self.change = "---"
        return True

    @measure(JUPITER_LOGGER)
    def do_analysis(self, db):
        st = time.time()
        if self.flag == "M":
            self.get_trigger_details(trigger_name='manual', db=db)
        elif self.flag == "S":
            self.get_trigger_details(trigger_name='sales_request', db=db)
        # print 'trigger details time ', st - time.time()
        st = time.time()
        # print self.__dict__
        trigger_status = self.check_trigger()
        # print 'checking trigger status time ', st - time.time()
        # st = time.time()
        dep_date_start = self.triggering_data['dep_date_start']
        dep_date_end = self.triggering_data['dep_date_end']
        # print 'Manual Trigger Being Raised for - '
        # print json.dumps(self.old_doc_data)
        id = self.generate_trigger_new(trigger_status=trigger_status,
                                       dep_date_start=dep_date_start,
                                       dep_date_end=dep_date_end,
                                       db=db)
        analyze(id, db=db)
        # print 'to generate trigger time', st - time.time()


if __name__ == '__main__':
    client = mongo_client()
    db=client[JUPITER_DB]
    import time
    st = time.time()
    print st
    obj = ManualTrigger(pos='UAE',
                        origin='DXB',
                        destination='CMB',
                        compartment='Y',
                        dep_date_start='2017-10-27',
                        dep_date_end='2017-11-27',
                        reason='Manual Trigger Test',
                        work_package_name='WP1',
                        flag='M')
    obj.do_analysis(db=db)
    print "Manual trigger generated and sent to queue!"
    print time.time() - st