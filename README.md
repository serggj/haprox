INTRO
=====
Обертка для изменения состояния backenda haproxy перед рестартом.    
Для работы нужно: 
- добавить праметры в конфиг
- сконфигурировать socket для haproxy [stats socket {host}:{port} level admin](http://cbonte.github.io/haproxy-dconv/1.6/management.html#9.2)
- разешить подключения через firewall


### Requirements
* python >= 2.7
* python-yaml

        apt-get install python python-yaml
        pip install pyapi-gitlab --user


Options
-------
```
  -h, --help            show this help message and exit
  -f config.yml, --config-file config.yml
                        path to user config. Default /etc/haproxy_restart_wrapper/config.yml
  --version             show script version
```

Examples
--------
haproxy_restart_wrapper.py -f /etc/haproxy_restart_wrapper/config_local.yml
