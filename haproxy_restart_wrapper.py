#!/usr/bin/env python
from telnetlib import Telnet
import logging
import StringIO
import csv
import json
import sys
import time
import subprocess
import yaml
import argparse


# color logging
logging.addLevelName(
    logging.ERROR,
    "\033[1;31m%s\033[1;0m" % logging.getLevelName(logging.ERROR))
logging.addLevelName(
    logging.INFO,
    "\033[1;34m%s\033[1;0m" % logging.getLevelName(logging.INFO))
logging.addLevelName(
    logging.WARNING,
    "\033[1;33m%s\033[1;0m" % logging.getLevelName(logging.WARNING))


# log level
logging.basicConfig(level=logging.INFO)
MAIN_LOGGER = logging.getLogger('main')
TELNET_LOGGER = logging.getLogger('telnet')
MAIN_LOGGER.setLevel('INFO')
TELNET_LOGGER.setLevel('INFO')


# config path
parser = argparse.ArgumentParser(
    description="haproxy_restart_wrapper %(prog)s",
    epilog="epilog %(prog)s",
    prog="haproxy_restart_wrapper.py")


# functions
def get_params():
    parser.add_argument(
        "-f", "--config-file",
        dest="config_file",
        default='config.yml',
        metavar='config.yml',
        help="path to user config.\nDefault config.yml")

    args = parser.parse_args()
    return args


def retries(func):
    def wrapper(*args):
        retries = 3
        while retries >= 0:
            try:
                return func(*args)
            except StateMismatch:
                MAIN_LOGGER.warning(Haproxy.color_msg('warn', 'recheck'))
                retries -= 1
                if retries == 0:
                    msg = 'Check is not successful for {0} attempts'.format(retries)
                    raise RuntimeError(Haproxy.color_msg('err', msg))
                time.sleep(0.5)
    return wrapper


class StateMismatch(Exception):
    """ raise where state not equal"""
    pass


class Haproxy(object):
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
            'scur',
            'check_status'
        ]
        self.enable_states = {
            'ready': 'UP',
            'drain': 'DRAIN',
            'maint': 'MAINT',
        }

    @staticmethod
    def color_msg(level, msg):
        msg_color_err = '\x1b[31m {} \x1b[0m'
        msg_color_info = '\x1b[34m {} \x1b[0m'
        msg_color_warn = '\x1b[33m {} \x1b[0m'

        if level == 'info':
            return msg_color_info.format(msg)
        elif level == 'err':
            return msg_color_err.format(msg)
        elif level == 'warn':
            return msg_color_warn.format(msg)

    def _validate_state(self, state):
        try:
            self.enable_states[state]
        except KeyError:
            msg = "unsupported state {0} " \
                  "supported states are {1}" \
                  .format(state, self.enable_states.keys())
            raise RuntimeError(self.color_msg('err', msg))

    def _get_tl(self):
        try:
            return Telnet(self.host, self.port, self.timeout)
        except Exception as err:
            raise RuntimeError(self.color_msg('err', err))

    def _reconnect(self):
        try:
            self.tl.open(self.host, self.port, self.timeout)
        except Exception as err:
            raise RuntimeError(self.color_msg('err', err))

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
            TELNET_LOGGER.debug('send telnet data {}'.format(repr(data)))
        except Exception as err:
            raise RuntimeError(self.color_msg('err', err))

    def read_all(self, split=True, cut=None):
        try:
            res = self.tl.read_all()
            if res:
                TELNET_LOGGER.debug('data read succesfull')
                if cut:
                    res = res[cut:]
                if split:
                    res = self._split_data(res)
                return res
            else:
                TELNET_LOGGER.warning('no data fo read')
        except Exception as err:
            raise RuntimeError(self.color_msg('err', err))

    @staticmethod
    def parse(data, servers_names=[], backends=[], get_all=False):
        total_res = {}
        if get_all:
            for row in data:
                svname = row.get('svname')
                if svname:
                    if svname != 'BACKEND' and svname != 'FRONTEND':
                        label = row['pxname'] + '/' + svname
                        total_res[label] = row
        else:
            for row in data:
                for backend in backends:
                    if row.get('pxname', '') == backend:
                        for server_name in servers_names:
                            if row.get('svname', '') == server_name:
                                label = row['pxname'] + '/' + server_name
                                total_res[label] = row

        return total_res

    def _check_server_exists(self, backend, server):
        try:
            msg = 'Check server {0}/{1} exists'.format(backend, server)
            MAIN_LOGGER.info(self.color_msg('info', msg))
            self.parse_servers_stats()[backend + '/' + server]
            msg = 'Ok server {0} exist in backend {1}' \
                .format(server, backend)
            MAIN_LOGGER.info(self.color_msg('info', msg))
        except KeyError as err:
            msg = 'server {0} not found in backend {1}'.format(server, backend)
            MAIN_LOGGER.error(self.color_msg('err', msg))
            raise RuntimeError(self.color_msg('err', err))

    @retries
    def _check_server_state(self, backend, server, state):
            label = backend + '/' + server
            stats = self.parse_servers_stats()[label]
            status = stats['status']
            if self.enable_states[state] not in status:
#            if self.enable_states[state] not in 'sdasdasd':
                msg = 'state server {0} in backend {1} not changed to {2} \n' \
                      'current state is {3}'\
                    .format(server, backend, state, status)
                MAIN_LOGGER.warning(self.color_msg('warn', msg))
                raise StateMismatch
            else:
                msg = 'state server {0} in backend {1} changed to {2}'\
                    .format(server, backend, status)
                MAIN_LOGGER.info(self.color_msg('info', msg))

    def set_server_state(self, backend, server, state):
            self._validate_state(state)
            self._check_server_exists(backend, server)
            set_cmd = self.set_state_template.format(
                backend=backend, server=server, state=state)
            self.write(set_cmd)
            self._check_server_state(backend, server, state)

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

    def waiting_close_sessions(self, backend, server):
        msg = 'Waiting {0}/{1} sessions close' \
            .format(backend, server)
        MAIN_LOGGER.info(self.color_msg('info', msg))
        label = backend + '/' + server
        stats = self.parse_servers_stats()[label]
        sessions = int(stats['scur'])
        MAIN_LOGGER.info('curren sessions {0}'.format(sessions))
        while sessions > 0:
            TELNET_LOGGER.setLevel('WARNING')
            sys.stdout.write('.')
            sys.stdout.flush()
            time.sleep(1)
            stats = self.parse_servers_stats()[label]
            sessions = int(stats['scur'])
        sys.stdout.write('\n')
        TELNET_LOGGER.setLevel('INFO')
        MAIN_LOGGER.info(self.color_msg('info', 'waiting complete'))
        MAIN_LOGGER.info(
            self.color_msg('info', 'current sessions {0}'.format(sessions)))

    def json_states(self):
        self.json_out(self.get_servers_keys())
        #self.json_out(self.parse_servers_stats())

    def close(self):
        if self.tl:
            self.tl.close()

    def __del__(self):
        self.close()


def call_cmd(cmd):
    msg = 'run cmd "{}"'.format(cmd)
    MAIN_LOGGER.info(Haproxy.color_msg('info', msg))
    try:
        subprocess.check_call(cmd, shell=True)
    except subprocess.CalledProcessError as err:
        raise RuntimeError(Haproxy.color_msg('err', err))


def load_config(config_file):
    try:
        with open(config_file, 'r') as stream:
            config = yaml.load(stream)
        return config
    except IOError as err:
        msg = "Config file not found: {}".format(err)
        raise RuntimeError(Haproxy.color_msg('err', msg))


def main():
    config_file = get_params().config_file
    config = load_config(config_file)
    haproxy_params = config['haproxy_params']
    server_params = config['server_params']
    server_state_down = config['server_state_down']
    server_state_up = config['server_state_up']
    restart_cmd = config['restart_cmd']
    haproxy = Haproxy(**haproxy_params)
    haproxy.set_server_state(state=server_state_down, **server_params)
    haproxy.waiting_close_sessions(**server_params)
    call_cmd(restart_cmd)
    haproxy.set_server_state(state=server_state_up, **server_params)
    haproxy.json_states()


if __name__ == '__main__':
    main()
