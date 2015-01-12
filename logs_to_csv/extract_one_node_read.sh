#!/bin/bash
find /opt/export/push01d.mds.yandex.net/elliptics-storage*/elisto17g.mds.yandex.net/elliptics/ -type f -iname '*node-1.log-20141222*' | parallel --progress "zfgrep 'READ: client' {}" | cut -d" " -f 1,2,5,20 >/opt/export/key.elisto17g.list_201412
sort /opt/export/key.elisto17g.list_201412 > /opt/export/key.elisto17g.list_201412_sorted
