#!/bin/bash

[ ! -d '/tmp/cache' ] && mkdir -p /tmp/cache

cd ts-packages/web
HOSTNAME=0.0.0.0 exec node server.js
