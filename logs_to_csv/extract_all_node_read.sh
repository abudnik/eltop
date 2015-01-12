#!/bin/bash
find /opt/export/push01d.mds.yandex.net/elliptics-storage*/elisto*.mds.yandex.net/elliptics/ -type f -iname '*node-1.log-20141222*' | parallel --progress "zfgrep 'READ: client' {}" |cut -d" " -f 1,2,5,20 >/opt/export/key.list_201412
