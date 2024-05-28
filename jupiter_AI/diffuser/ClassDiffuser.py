############################################################
## Author: Prem Narasimhan
## First completion date: July 1, 2017
############################################################
import jupiter_AI.common.ClassErrorObject as errorClass
import datetime
import json
import time
import jupiter_AI.network_level_params as net
import inspect
import collections
import copy
import jupiter_AI.common.ClassErrorObject as error_class

from jupiter_AI.network_level_params import JUPITER_DB


class diffuser(object):
    def __init__(self, stepup_type, obj_diffuser_reference, list_of_dicts_base_farebases):
        self.stepup_type                    = stepup_type ## can be absolute or relative
        self.obj_diffuser_reference         = obj_diffuser_reference
        self.list_of_dicts_base_farebases   = list_of_dicts_base_farebases
        self.diffuser_ordering_numbers      = self.obj_diffuser_reference.get_diffuser_ordering_numbers()
        self.list_of_dicts_diffuser_output = []
        self.list_of_dicts_diffuser_output  = self.get_diffuser_output()

    def get_diffuser_output(self):
        list_of_dicts_diffuser_output=[]
        for i in range(len(self.list_of_dicts_base_farebases)):
            list_of_dicts_diffuser_output_for_one_base_farebasis = self.diffuse_one_base_farebasis(i)
                #print k
            dicts = {}
            for k in list_of_dicts_diffuser_output_for_one_base_farebasis:
                dicts[k['farebasis']] = k['price']
            ##print dicts
            for dict_row in list_of_dicts_diffuser_output_for_one_base_farebasis:
                list_of_dicts_diffuser_output.append(dict_row)
        return list_of_dicts_diffuser_output

    def diffuse_one_base_farebasis(self,i3):
        dict_base_farebasis     = self.list_of_dicts_base_farebases[i3]

        list_of_dicts_diffuser_output_for_one_base_farebasis=[]
        self.calculate_prices(dict_base_farebasis, list_of_dicts_diffuser_output_for_one_base_farebasis)
        return list_of_dicts_diffuser_output_for_one_base_farebasis

    def calculate_prices(self, dict_base_farebasis, list_of_dicts_diffuser_output_for_one_base_farebasis):
        base_farebasis                  = dict_base_farebasis['farebasis']
        # print 'base_farebasis', base_farebasis
        base_fare                       = dict_base_farebasis['price']
        base_fare_index                 = self.get_basefare_index(base_farebasis)
        # print 'base_farebasis', base_farebasis
        # print 'base_fare_index111', base_fare_index
        ntiers                          = len(self.diffuser_ordering_numbers)
        if base_fare_index > 0:
            for i in range(base_fare_index):     ## selldown
                i_countdown                 = base_fare_index-1-i
                diffuser_row_index          = i_countdown
                diffuser_ordering_number    = self.diffuser_ordering_numbers[i_countdown]
                if self.stepup_type         =='absolute':
                    base_fare               = dict_base_farebasis['price']
                if self.stepup_type         =='relative':
                    if i_countdown == base_fare_index-1:
                        base_fare           = dict_base_farebasis['price']
                    else:
                        base_fare           = prv_price                       
                ordering_name               = self.obj_diffuser_reference.get_ordering_name(diffuser_ordering_number)
                super_rbd                   = self.obj_diffuser_reference.get_super_rbd_name(diffuser_ordering_number)
                dict_arguments              = {'farebasis': base_farebasis, 'ordering_name': ordering_name, 'diffuser_ordering_number': diffuser_ordering_number, 'super_rbd': super_rbd}
                farebasis                   = self.get_resultant_farebasis_code(dict_arguments)
                oneway_or_return            = farebasis[1].upper()
                price                       = self.calculate_price(diffuser_row_index, diffuser_ordering_number, base_fare, oneway_or_return, 'selldown')
                prv_price                   = price
                
                self.append_dict_results(list_of_dicts_diffuser_output_for_one_base_farebasis, dict_base_farebasis, diffuser_ordering_number, price, farebasis)
        # print 'base_fare_index', base_fare_index
        diffuser_ordering_number    = self.diffuser_ordering_numbers[base_fare_index]
        ordering_name               = self.obj_diffuser_reference.get_ordering_name(diffuser_ordering_number)
        super_rbd                   = self.obj_diffuser_reference.get_super_rbd_name(diffuser_ordering_number)
        dict_arguments              = {'farebasis': base_farebasis, 'ordering_name': ordering_name, 'diffuser_ordering_number': diffuser_ordering_number, 'super_rbd': super_rbd}
        farebasis                   = self.get_resultant_farebasis_code(dict_arguments)

        self.append_dict_results(list_of_dicts_diffuser_output_for_one_base_farebasis, dict_base_farebasis, diffuser_ordering_number, dict_base_farebasis['price'], farebasis)
        if base_fare_index < ntiers-1:
            for i in range(base_fare_index, len(self.diffuser_ordering_numbers)-1):     ## sellup
                diffuser_row_index          = i
                diffuser_ordering_number    = self.diffuser_ordering_numbers[i+1]
                if self.stepup_type         =='absolute':
                    base_fare               = dict_base_farebasis['price']
                if self.stepup_type         =='relative':
                    if i == base_fare_index:
                        base_fare           = dict_base_farebasis['price']
                    else:
                        base_fare           = prv_price                       
                ordering_name               = self.obj_diffuser_reference.get_ordering_name(diffuser_ordering_number)
                super_rbd                   = self.obj_diffuser_reference.get_super_rbd_name(diffuser_ordering_number)
                dict_arguments              = {'farebasis': base_farebasis, 'ordering_name': ordering_name, 'diffuser_ordering_number': diffuser_ordering_number, 'super_rbd': super_rbd}
                farebasis                   = self.get_resultant_farebasis_code(dict_arguments)
                oneway_or_return            = farebasis[1].upper()
                price                       = self.calculate_price(diffuser_row_index, diffuser_ordering_number, base_fare, oneway_or_return, 'sellup')
                prv_price                   = price
                
                self.append_dict_results(list_of_dicts_diffuser_output_for_one_base_farebasis, dict_base_farebasis, diffuser_ordering_number, price, farebasis)

    def get_basefare_index(self, base_farebasis):
        pass
    
    def calculate_price(self, diffuser_row_index, diffuser_ordering_number, base_fare, oneway_or_return, sellup_or_down):
        if oneway_or_return=='O':
            price=self.calculate_price2(diffuser_row_index, diffuser_ordering_number, base_fare, 'o_amt_or_perc', 'o_amt', sellup_or_down)
            return price
        if oneway_or_return=='R':
            price=self.calculate_price2(diffuser_row_index, diffuser_ordering_number, base_fare, 'r_amt_or_perc', 'r_amt', sellup_or_down)
            return price
        e = error_class.ErrorObject(error_class.ERRORLEVEL2, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
        e.append_to_error_list("fare is neither ONEWAY or RETURN")
        raise e
        
    def calculate_price2(self, diffuser_row_index, diffuser_ordering_number, base_fare, amt_or_perc, amt, sellup_or_down):
        if sellup_or_down=='sellup':
            return self.calculate_price2_sellup(diffuser_row_index, diffuser_ordering_number, base_fare, amt_or_perc, amt)
        else:
            return self.calculate_price2_selldown(diffuser_row_index, diffuser_ordering_number, base_fare, amt_or_perc, amt)

    def calculate_price2_sellup(self,diffuser_row_index, diffuser_ordering_number, base_fare, amt_or_perc, amt):
        diffuser_row_sellup=self.obj_diffuser_reference.dict_diffuser_ref_table[self.diffuser_ordering_numbers[diffuser_row_index]]
        diffuser_row=self.obj_diffuser_reference.dict_diffuser_ref_table[diffuser_ordering_number]
        if diffuser_row[amt_or_perc]=='perc':
            price=base_fare*(1+diffuser_row_sellup[amt]/100.0)
            return price
        if diffuser_row[amt_or_perc]=='amt':
            price=base_fare + diffuser_row_sellup[amt]
            return price
        e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
        e.append_to_error_list("amt type=" + amt_or_perc + ": neither PERC or AMT")
        raise e

    def calculate_price2_selldown(self,diffuser_row_index, diffuser_ordering_number, base_fare, amt_or_perc, amt):
        diffuser_row=self.obj_diffuser_reference.dict_diffuser_ref_table[diffuser_ordering_number]
        diffuser_row_selldown=diffuser_row
        if diffuser_row[amt_or_perc]=='perc':
            price=base_fare/(1+diffuser_row_selldown[amt]/100.0)
            return price
        if diffuser_row[amt_or_perc]=='amt':
            price=base_fare - diffuser_row_selldown[amt]
            return price
        e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
        e.append_to_error_list("amt type=" + amt_or_perc + ": neither PERC or AMT")
        raise e

    def append_dict_results(self, list_of_dicts_diffuser_output_for_one_base_farebasis, dict_base_farebasis, diffuser_ordering_number, price, farebasis):
            list_of_dicts_diffuser_output_for_one_base_farebasis.append({'diffuser_ordering_number': diffuser_ordering_number,
                         'pos': dict_base_farebasis['pos'],
                         'origin': dict_base_farebasis['origin'],
                         'destination': dict_base_farebasis['destination'],
                         'compartment': dict_base_farebasis['compartment'],
                         'country': dict_base_farebasis['country'],
                         'region': dict_base_farebasis['region'],
                         'price': price,
                         'farebasis': farebasis,
                         })


    def get_resultant_farebasis_code(self, dict_arguments):
        pass

    def get_module_name(self):
        return inspect.stack()[1][3]

    def get_arg_lists(self, frame):
        """
        function used to get the list of arguments of the function
        where it is called
        """
        args, _, _, values = inspect.getargvalues(frame)
        argument_name_list=[]
        argument_value_list=[]
        for k in args:
            argument_name_list.append(k)
            argument_value_list.append(values[k])
        return argument_name_list, argument_value_list

