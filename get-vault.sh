#!/bin/bash

./vault login -address="$3" -no-print $(./vault write -address="$3" -field=token auth/approle/login role_id="$1" secret_id="$2")
echo "$3"
for file in $(./vault kv get -address="$3" -format=json stg/stg-vmonitor-platform/flink-metric-forwarder | jq '.data.data | keys | .[]'); do echo $(echo $file|sed "s/^.//;s/.$//") >> temp; done
while read file; do ./vault kv get -address="$3" -field=$file stg/stg-vmonitor-platform/flink-metric-forwarder > $file; done <temp
