import requests
import pandas as pd
from time import sleep
from cron_descriptor import ExpressionDescriptor


class ApiEngine:
    """Класс для автоматизации задач по Nifi Api"""

    def __init__(self, base_url, log=False, username=None, password=None):
        self.log = log
        self.time = 5
        self.url_get_processor = "{0}/nifi-api/processors/*".format(base_url)
        self.url_get_group = "{0}/nifi-api/process-groups/*".format(base_url)
        self.url_pars_processor = "{0}/nifi-api/process-groups/*/processors".format(base_url)
        self.url_pars_proc_group = "{0}/nifi-api/process-groups/*/process-groups".format(base_url)
        self.url_start_stop = "{0}/nifi-api/processors/*/run-status".format(base_url)
        self.url_label = "{0}/nifi-api/process-groups/*/labels".format(base_url)
        if username:
            self.access = requests.post(url='{0}/nifi-api/access/token'.format(base_url),
                                        data={'username': username, 'password': password})
            self.token = self.access.text
        else:
            self.token = ''

    def get_ignore_group(self, id_parent_process_group, ignore_name):

        """Функция для нахождения неиспользуемых групп процессоров"""

        self.ignore_list = list()
        self.x_up = 0
        self.y_up = 0
        self.x_down = 0
        self.y_down = 0

        get = requests.get(self.url_label.replace('*', id_parent_process_group),
                           headers={"Authorization": "Bearer " + self.token})

        for label in get.json()['labels']:
            if label['component']['label'] == ignore_name:
                self.x_up = float(label['component']['position']['x'])
                self.y_up = float(label['component']['position']['y'])
                self.x_down = float(label['component']['position']['x']) + float(label['component']['width'])
                self.y_down = float(label['component']['position']['y']) + float(label['component']['height'])

        if self.x_up - self.x_down == 0:
            print('Внимание не было найдено меток "{0}" в заданной группе!'.format(ignore_name))
            return self.ignore_list

        get = requests.get(self.url_pars_proc_group.replace('*', id_parent_process_group),
                           headers={"Authorization": "Bearer " + self.token})

        for group in get.json()['processGroups']:
            x_group = float(group['component']['position']['x'])
            y_group = float(group['component']['position']['y'])
            if (x_group < self.x_down) and (y_group < self.y_down) and (y_group > self.y_up) and (x_group > self.x_up):
                self.ignore_list.append(group['component']['name'])

        return self.ignore_list

    def get_process_group(self, id_parent_process_group, ignore_name=''):

        """ Функция для нахождения списка всех групп процессов в группе"""

        list_process_group = []

        if len(ignore_name) > 0:
            ignore_list = self.get_ignore_group(id_parent_process_group, ignore_name)
        else:
            ignore_list = list()

        get = requests.get(self.url_pars_proc_group.replace('*', id_parent_process_group),
                           headers={"Authorization": "Bearer " + self.token})

        for group in get.json()['processGroups']:
            if group['component']['name'] in ignore_list:
                pass
            else:
                group_name = group['component']['name'] + '+' + group['id']
                list_process_group.append([group_name, group['id'], group['uri']])
        return list_process_group

    def get_processor_in_group(self, id_parent_process_group, path_process_group):

        """ Функция для нахождения списка всех процессоров в группе"""

        get = requests.get(self.url_pars_processor.replace('*', id_parent_process_group),
                           headers={"Authorization": "Bearer " + self.token})
        list_processor = []
        for proc in get.json()['processors']:

            type_proc = proc['component']['type'].split('.')

            try:
                target = proc['component']['config']['properties']['target']
            except KeyError:
                target = 'No target'

            sheduling_type = proc['component']['config']['schedulingStrategy']
            if sheduling_type == 'CRON_DRIVEN':
                sheduling_time = proc['component']['config']['schedulingPeriod'] + ' - ' + str(ExpressionDescriptor(
                    proc['component']['config']['schedulingPeriod'], use_24hour_time_format=True))
            else:
                sheduling_time = proc['component']['config']['schedulingPeriod']

            list_processor.append(
                [proc['component']['name'], proc['id'], path_process_group, proc['component']['state'], type_proc[-1],
                 proc['revision']['version'], target, sheduling_type, sheduling_time, proc['uri']])
        return list_processor

    def preOrder(self, id_parent_process_group, name, result_dict, ignore_name=''):

        """Функция для рекурсивного обхода групп процессов"""

        self.list_proc_group = self.get_process_group(id_parent_process_group, ignore_name=ignore_name)
        self.dict_proc_group = {}

        for proc in self.list_proc_group:
            self.dict_proc_group[proc[0]] = {'id': proc[1], 'group_in': None}

        result_dict['group_in'] = self.dict_proc_group

        for key, value in result_dict['group_in'].items():
            sleep(self.time)
            self.preOrder(value['id'], key, value, ignore_name)

    def get_dict_processor_group(self, id_parent_process_group, ignore_name=''):

        """Функция для извлечения словаря - дерево, из рекурсивного обхода группы процессов"""

        parent_group_name = requests.get(self.url_get_group.replace('*', id_parent_process_group),
                                         headers={"Authorization": "Bearer " + self.token})
        self.name = parent_group_name.json()['component']['name'] + '+' + parent_group_name.json()['component']['id']

        self.result_dict = {self.name: {'id': id_parent_process_group, 'group_in': None}}

        self.preOrder(id_parent_process_group, self.name, self.result_dict[self.name], ignore_name)

        return self.result_dict

    def get_abs_path(self, target_dict, key):

        """Функция для нахождения пути до процесса"""

        if not isinstance(target_dict, dict):
            return None
        if key in target_dict.keys():
            return key
        ans = None
        for json_key in target_dict.keys():
            r = self.get_abs_path(target_dict[json_key], key)
            if r is None:
                continue
            else:
                ans = "{}.{}".format(json_key, r)
        return ans

    def extract_processor(self, etalon, dict_group, list_temp):

        """Функция сбора процессов в единый список"""

        for key, value in dict_group.items():
            path = self.get_abs_path(etalon, key)
            path = list(path.split('.group_in.'))
            path = [x[:x.find('+')] for x in path]
            path = '->'.join(path)
            if self.log:
                print(path)
            sleep(self.time / 2)
            list_res = self.get_processor_in_group(value['id'], path)

            if len(list_res) != 0:
                for gf in list_res:
                    list_temp.append(gf)

            self.extract_processor(etalon, value['group_in'], list_temp)

    def get_all_processor(self, id_parent_process_group, ignore_name=''):

        """Функция загрузки их в единый DataFrame"""

        dict_proc = self.get_dict_processor_group(id_parent_process_group, ignore_name=ignore_name)

        col = ['Name', 'ID', 'Parent_group', 'State', 'Type', 'Version', 'Target',
               'Sheduling_type', 'Sheduling_time', 'Uri']
        list_temp = []

        self.extract_processor(dict_proc, dict_proc, list_temp)

        self.result_df = pd.DataFrame(list_temp, columns=col)

        return self.result_df

    def get_version(self, id_proc):

        """"Функция получения версии процессоров (пока не актуально)"""

        get = requests.get(self.url_get_processor.replace('*', id_proc),
                           headers={"Authorization": "Bearer " + self.token})
        return get.json()["revision"]["version"]

    def change_state(self, id_proc, state, version):

        """Функция изменения состояния процессоров"""

        self.config_js = {"revision": {"clientId": '000api000', "version": version},
                          "state": state,
                          "disconnectedNodeAcknowledged": "true"}
        res = requests.put(
            url=(self.url_start_stop.replace('*', id_proc)),
            headers={"Authorization": "Bearer " + self.token},
            json=self.config_js)
        print(id_proc, " - ", state, " - ", res)
        sleep(5)

    def start_processor(self, df_change_state):

        """Функция запуска процессоров"""

        if self.token:
            token = self.token
        for index, proc in df_change_state.iterrows():
            if proc['State'] == "STOPPED":
                self.change_state(id_proc=proc['ID'], state="RUNNING", version=proc['Version'])
                proc['Version'] = self.get_version(id_proc=proc['ID'])
                proc['State'] = "RUNNING"

            elif proc[3] == "DISABLED":
                self.change_state(id_proc=proc['ID'], state="STOPPED", version=proc['Version'])
                proc['Version'] = self.get_version(id_proc=proc['ID'])
                proc['State'] = "STOPPED"
                self.change_state(id_proc=proc['ID'], state="RUNNING", version=proc['Version'])
                proc['Version'] = self.get_version(id_proc=proc['ID'])
                proc['State'] = "RUNNING"

    def stop_processor(self, df_change_state):

        """Функция остановки и запуска процессоров"""

        for index, proc in df_change_state.iterrows():
            if proc['State'] == "RUNNING":
                self.change_state(id_proc=proc['ID'], state="STOPPED", version=proc['Version'])
                proc['Version'] = self.get_version(id_proc=proc['ID'])
                proc['State'] = "STOPPED"

            elif proc['State'] == "DISABLED":
                self.change_state(id_proc=proc['ID'], state="STOPPED", version=proc['Version'])
                proc['Version'] = self.get_version(id_proc=proc['ID'])
                proc['State'] = "STOPPED"

    def disabel_processor(self, df_change_state):

        """Функция отключения процессоров"""

        for index, proc in df_change_state.iterrows():
            if proc['State'] == "STOPPED":
                self.change_state(id_proc=proc['ID'], state="DISABLED", version=proc['Version'])
                proc['Version'] = self.get_version(id_proc=proc['ID'])
                proc['State'] = "DISABLED"

            elif proc['State'] == "RUNNING":
                self.change_state(id_proc=proc['ID'], state="STOPPED", version=proc['Version'])
                proc['Version'] = self.get_version(id_proc=proc['ID'])
                proc['State'] = "STOPPED"
                self.change_state(id_proc=proc['ID'], state="DISABLED", version=proc['Version'])
                proc['Version'] = self.get_version(id_proc=proc['ID'])
                proc['State'] = "DISABLED"
