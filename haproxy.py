#!/usr/bin/env python
from telnetlib import Telnet
import logging
import StringIO
import csv
import json


logging.basicConfig(level=logging.INFO)


class Telnet_Helper(object):
    def __init__(self, host, port, timeout):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.tl = self._get_tl()
        self.set_state_template = \
            'set server {backend}/{server} state {state}\n'
        self.keys_list = [
            'pxname',
            'svname',
            'status',
            'scur'
        ]
        self.enable_states = {
            'ready': 'UP',
            'drain': 'DRAIN',
            'maint': 'MAINT',
        }

    def _validate_state(self, state):
        try:
            self.enable_states[state]
        except KeyError:
            msg = "unsupported state {0} "\
                  "supported states are {1}"\
                  .format(state, self.enable_states.keys())
            raise RuntimeError(msg)

    def _get_tl(self):
        try:
            return Telnet(self.host, self.port, self.timeout)
        except Exception as err:
            raise RuntimeError(err)

    def _reconnect(self):
        try:
            self.tl.open(self.host, self.port, self.timeout)
        except Exception as err:
            raise RuntimeError(err)

    def _split_data(self, data, delimiter='\n'):
        return data.split(delimiter)

    @staticmethod
    def read_str_as_csv(data):
        f = StringIO.StringIO(data)
        reader = csv.DictReader(f, delimiter=',')
        return [row for row in reader]

    def write(self, data):
        try:
            self._reconnect()
            self.tl.write(data)
            logging.info('send telnet data {}'.format(repr(data)))
        except Exception as err:
            raise RuntimeError(err)

    def read_all(self, split=True, cut=None):
        try:
            res = self.tl.read_all()
            if res:
                logging.info('data read succesfull')
                if cut:
                    res = res[cut:]
                if split:
                    res = self._split_data(res)
                return res
            else:
                logging.warning('no data fo read')
        except Exception as err:
            raise RuntimeError(err)

    @staticmethod
    def parse(data, servers_names=[], backends=[], get_all=False):
        total_res = {}
        if get_all:
            for row in data:
                svname = row.get('svname')
                if svname:
                    if svname != 'BACKEND' and svname != 'FRONTEND':
                        label = row['pxname'] + '+' + svname
                        total_res[label] = row
        else:
            for row in data:
                for backend in backends:
                    if row.get('pxname', '') == backend:
                        for server_name in servers_names:
                            if row.get('svname', '') == server_name:
                                label = row['pxname'] + '+' + server_name
                                total_res[label] = row

        return total_res

    def _check_server_exists(self, backend, server):
        try:
            logging.info('check server exists')
            self.parse_servers_stats()[backend + '+' + server]
            logging.info('Ok server {0} exist in backend {1}'\
                .format(server, backend)
            )
        except KeyError as err:
            logging.error(
                'server {0} not found in backend {1}'.format(server, backend))
            raise RuntimeError(err)

    def _check_server_status(self, backend, server, state):
            label = backend + '+' + server
            stats = self.parse_servers_stats()[label]
            status = stats['status']
            if self.enable_states[state] not in status:
                msg = 'state server {0} in backend {1} not changed to {2} \n'\
                      'current state id {3}'\
                    .format(server, backend, state, status)
                raise RuntimeError(msg)
            else:
                msg = 'state server {0} in backend {1} changed to {2}'\
                    .format(server, backend, status)
                logging.info(msg)

    def set_server_state(self, backend, server, state):
            self._validate_state(state)
            self._check_server_exists(backend, server)
            set_cmd = self.set_state_template.format(
                backend=backend, server=server, state=state)
            self.write(set_cmd)
            self._check_server_status(backend, server, state)

    def servers_stats(self):
        self.write('show stat\n')
        res = self.read_all(False, 2)
        return self.read_str_as_csv(res)

    def parse_servers_stats(self):
        return self.parse(
            self.servers_stats(),
            servers_names=[],
            backends=[], get_all=True
        )

    def get_servers_keys(self):
        st = self.parse_servers_stats()

        return {
            label: {
                k: st[label].get(k) for k in self.keys_list
            } for label in st
        }

    @staticmethod
    def json_out(data):
        print json.dumps(data, indent=4)

    def waitind_close_sessions(self, backend, server):
        loggin.info('witing {0}/{1} sessions close')
        logging.info('curren sessions')
        label = backend + '+' + server
        stats = self.parse_servers_stats()[label]


    def close(self):
        if self.tl:
            self.tl.close()

    def __del__(self):
        self.close()


# set server <backend>/<server> state [ ready | drain | maint ]
if __name__ == '__main__':
    params = {
        'host': '172.27.247.87',
        'port': 2525,
        'timeout': 5
    }
    th = Telnet_Helper(**params)
    #th.write('show stat\n')
    #r = th.read_all(False, 2)
    #r = th.read_str_as_csv(r)
    #p = th.parse(r, ['test1', 'test2'], ['def_b'], True)
    th.json_out(th.get_servers_keys())
    th.set_server_state('def_b', 'test1', 'drain')
    #th.set_server_state('def_b', 'test1', 'ready')
