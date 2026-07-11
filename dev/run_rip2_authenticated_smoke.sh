#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "$ROOT_DIR"

DIST="${RIP2_DIST:-distribution/target/rocketmq-5.5.0/rocketmq-5.5.0}"
RUN_ID="${RIP2_AUTH_SMOKE_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
RESULT_DIR="target/rip2-auth-smoke-$RUN_ID"
STORE_DIR="$RESULT_DIR/store"
BROKER_CONFIG="$RESULT_DIR/broker.conf"
PROXY_CONFIG="$RESULT_DIR/rmq-proxy.json"
SUPER_ACCESS_KEY="rocketmq2"
SUPER_SECRET_KEY="12345678"
LIST_ACCESS_KEY="rip2-list"
LIST_SECRET_KEY="ListSecret123"
GET_ACCESS_KEY="rip2-get"
GET_SECRET_KEY="GetSecret123"
PORTS=(9876 8081 8082 10911)
CLOSED_PORT_EVIDENCE=(
  port-9876-closed
  port-8081-closed
  port-8082-closed
  port-10911-closed
)
NS_PID=""
PROXY_PID=""
NS_JAVA_PID=""
PROXY_JAVA_PID=""

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

port_is_open() {
  lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
}

port_owner_pid() {
  lsof -nP -t -iTCP:"$1" -sTCP:LISTEN 2>/dev/null | head -1
}

assert_ports_closed() {
  local failed=0
  local index
  local port
  for index in "${!PORTS[@]}"; do
    port="${PORTS[$index]}"
    if port_is_open "$port"; then
      echo "port-$port-open"
      failed=1
    else
      echo "${CLOSED_PORT_EVIDENCE[$index]}"
    fi
  done
  return "$failed"
}

cleanup() {
  local original_status=$?
  local cleanup_status=0
  trap - EXIT INT TERM
  set +e
  [ -n "$PROXY_JAVA_PID" ] && kill "$PROXY_JAVA_PID" 2>/dev/null
  [ -n "$NS_JAVA_PID" ] && kill "$NS_JAVA_PID" 2>/dev/null
  [ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null
  [ -n "$NS_PID" ] && kill "$NS_PID" 2>/dev/null
  [ -n "$PROXY_PID" ] && wait "$PROXY_PID" 2>/dev/null
  [ -n "$NS_PID" ] && wait "$NS_PID" 2>/dev/null
  local attempt
  for attempt in $(seq 1 60); do
    local open_count=0
    local port
    for port in "${PORTS[@]}"; do
      port_is_open "$port" && open_count=$((open_count + 1))
    done
    [ "$open_count" -eq 0 ] && break
    sleep 1
  done
  if port_is_open 8082 || port_is_open 10911; then
    [ -n "$PROXY_JAVA_PID" ] && kill -KILL "$PROXY_JAVA_PID" 2>/dev/null
  fi
  if port_is_open 9876; then
    [ -n "$NS_JAVA_PID" ] && kill -KILL "$NS_JAVA_PID" 2>/dev/null
  fi
  for attempt in $(seq 1 10); do
    local remaining=0
    local port
    for port in "${PORTS[@]}"; do
      port_is_open "$port" && remaining=$((remaining + 1))
    done
    [ "$remaining" -eq 0 ] && break
    sleep 1
  done
  assert_ports_closed || cleanup_status=1
  if [ "$original_status" -ne 0 ]; then
    exit "$original_status"
  fi
  exit "$cleanup_status"
}

wait_for_proxy() {
  local attempt
  for attempt in $(seq 1 90); do
    if rg -q "rocketmq-proxy startup successfully" "$RESULT_DIR/proxy.log"; then
      local open_count=0
      local port
      for port in "${PORTS[@]}"; do
        port_is_open "$port" && open_count=$((open_count + 1))
      done
      if [ "$open_count" -eq "${#PORTS[@]}" ]; then
        NS_JAVA_PID="$(port_owner_pid 9876)"
        PROXY_JAVA_PID="$(port_owner_pid 8082)"
        if [ -z "$NS_JAVA_PID" ] || [ -z "$PROXY_JAVA_PID" ] \
          || [ "$NS_JAVA_PID" = "$PROXY_JAVA_PID" ]; then
          echo "cannot identify isolated smoke Java processes" >&2
          return 1
        fi
        return 0
      fi
    fi
    if ! kill -0 "$PROXY_PID" "$NS_PID" 2>/dev/null; then
      tail -n 160 "$RESULT_DIR/proxy.log" >&2 || true
      return 1
    fi
    sleep 1
  done
  tail -n 160 "$RESULT_DIR/proxy.log" >&2 || true
  return 1
}

signed_call() {
  local access_key="$1"
  local secret_key="$2"
  local method="$3"
  local payload="$4"
  local output="$5"
  local date_time
  local signature
  date_time="$(date -u +%Y%m%dT%H%M%SZ)"
  signature="$(printf '%s' "$date_time" \
    | openssl dgst -sha1 -hmac "$secret_key" -binary \
    | xxd -p -c 256)"
  grpcurl -plaintext \
    -import-path ../rocketmq-apis \
    -proto apache/rocketmq/v2/admin.proto \
    -H "x-mq-date-time: $date_time" \
    -H "authorization: MQv2-HMAC-SHA1 Credential=$access_key, SignedHeaders=x-mq-date-time, Signature=$signature" \
    -d "$payload" \
    127.0.0.1:8082 \
    "apache.rocketmq.v2.ProxyAdminService/$method" \
    > "$output" 2>&1
}

assert_status() {
  local output="$1"
  local expected="$2"
  python3 - "$output" "$expected" <<'PY'
import json
import sys

path, expected = sys.argv[1:]
with open(path, encoding="utf-8") as stream:
    payload = json.load(stream)
actual = payload.get("status", {}).get("code")
if actual != expected:
    raise SystemExit(f"{path}: expected status {expected}, got {actual}: {payload}")
print(f"{path}: {actual}")
PY
}

test -f pom.xml
test -f ../rocketmq-apis/apache/rocketmq/v2/admin.proto
test -x "$DIST/bin/mqnamesrv"
test -x "$DIST/bin/mqproxy"
test -x "$DIST/bin/mqadmin"
require_command grpcurl
require_command openssl
require_command xxd
require_command lsof
require_command rg
require_command python3

if [ -e "$RESULT_DIR" ]; then
  echo "result directory already exists: $RESULT_DIR" >&2
  exit 1
fi

for port in "${PORTS[@]}"; do
  if port_is_open "$port"; then
    echo "required smoke port is already in use: $port" >&2
    exit 1
  fi
done

mkdir -p "$RESULT_DIR"

cat > "$BROKER_CONFIG" <<EOF
brokerClusterName=DefaultCluster
brokerName=rip2-auth-smoke-broker
brokerId=0
brokerIP1=127.0.0.1
brokerIP2=127.0.0.1
listenPort=10911
deleteWhen=04
fileReservedTime=1
brokerRole=ASYNC_MASTER
flushDiskType=ASYNC_FLUSH
storePathRootDir=$STORE_DIR
authenticationEnabled=true
authenticationProvider=org.apache.rocketmq.auth.authentication.provider.DefaultAuthenticationProvider
authenticationMetadataProvider=org.apache.rocketmq.auth.authentication.provider.LocalAuthenticationMetadataProvider
authorizationEnabled=true
authorizationProvider=org.apache.rocketmq.auth.authorization.provider.DefaultAuthorizationProvider
authorizationMetadataProvider=org.apache.rocketmq.auth.authorization.provider.LocalAuthorizationMetadataProvider
initAuthenticationUser={"username":"$SUPER_ACCESS_KEY","password":"$SUPER_SECRET_KEY"}
EOF

cat > "$PROXY_CONFIG" <<EOF
{
  "rocketMQClusterName": "DefaultCluster",
  "namesrvAddr": "127.0.0.1:9876",
  "proxyMode": "local",
  "brokerConfigPath": "$BROKER_CONFIG",
  "enableProxyAdminGrpcServer": true,
  "proxyAdminGrpcServerPort": 8082,
  "authenticationEnabled": true,
  "authenticationProvider": "org.apache.rocketmq.auth.authentication.provider.DefaultAuthenticationProvider",
  "authenticationMetadataProvider": "org.apache.rocketmq.proxy.auth.ProxyAuthenticationMetadataProvider",
  "authorizationEnabled": true,
  "authorizationProvider": "org.apache.rocketmq.auth.authorization.provider.DefaultAuthorizationProvider",
  "authorizationMetadataProvider": "org.apache.rocketmq.proxy.auth.ProxyAuthorizationMetadataProvider"
}
EOF

trap cleanup EXIT
trap 'exit 130' INT TERM

sh "$DIST/bin/mqnamesrv" > "$RESULT_DIR/namesrv.log" 2>&1 &
NS_PID=$!
sh "$DIST/bin/mqproxy" \
  -pc "$PROXY_CONFIG" \
  -pm local \
  -n 127.0.0.1:9876 \
  > "$RESULT_DIR/proxy.log" 2>&1 &
PROXY_PID=$!
wait_for_proxy

signed_call "$SUPER_ACCESS_KEY" "$SUPER_SECRET_KEY" ListClients \
  '{"page_num":1,"page_size":10}' "$RESULT_DIR/super-list.json"
assert_status "$RESULT_DIR/super-list.json" OK

grpcurl -plaintext \
  -import-path ../rocketmq-apis \
  -proto apache/rocketmq/v2/admin.proto \
  -d '{"page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClients \
  > "$RESULT_DIR/unsigned-list.json" 2>&1
assert_status "$RESULT_DIR/unsigned-list.json" UNAUTHORIZED
rg -q "username cannot be null\." "$RESULT_DIR/unsigned-list.json"

BAD_DATE_TIME="$(date -u +%Y%m%dT%H%M%SZ)"
grpcurl -plaintext \
  -import-path ../rocketmq-apis \
  -proto apache/rocketmq/v2/admin.proto \
  -H "x-mq-date-time: $BAD_DATE_TIME" \
  -H "authorization: MQv2-HMAC-SHA1 Credential=$SUPER_ACCESS_KEY, SignedHeaders=x-mq-date-time, Signature=0000000000000000000000000000000000000000" \
  -d '{"page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClients \
  > "$RESULT_DIR/bad-signature-list.json" 2>&1
assert_status "$RESULT_DIR/bad-signature-list.json" UNAUTHORIZED
rg -q "check signature failed\." "$RESULT_DIR/bad-signature-list.json"

sh "$DIST/bin/mqadmin" createUser \
  -b 127.0.0.1:10911 -u "$LIST_ACCESS_KEY" -p "$LIST_SECRET_KEY" -t Normal \
  > "$RESULT_DIR/create-list-user.log" 2>&1
sh "$DIST/bin/mqadmin" createAcl \
  -b 127.0.0.1:10911 -s "User:$LIST_ACCESS_KEY" \
  -r Admin:proxy.admin.client -a List -d Allow \
  > "$RESULT_DIR/create-list-acl.log" 2>&1
sh "$DIST/bin/mqadmin" createUser \
  -b 127.0.0.1:10911 -u "$GET_ACCESS_KEY" -p "$GET_SECRET_KEY" -t Normal \
  > "$RESULT_DIR/create-get-user.log" 2>&1
sh "$DIST/bin/mqadmin" createAcl \
  -b 127.0.0.1:10911 -s "User:$GET_ACCESS_KEY" \
  -r Admin:proxy.admin.client -a Get -d Allow \
  > "$RESULT_DIR/create-get-acl.log" 2>&1
rg -q "create user to 127\.0\.0\.1:10911 success\." "$RESULT_DIR/create-list-user.log"
rg -q "create acl to 127\.0\.0\.1:10911 success\." "$RESULT_DIR/create-list-acl.log"
rg -q "create user to 127\.0\.0\.1:10911 success\." "$RESULT_DIR/create-get-user.log"
rg -q "create acl to 127\.0\.0\.1:10911 success\." "$RESULT_DIR/create-get-acl.log"

signed_call "$LIST_ACCESS_KEY" "$LIST_SECRET_KEY" ListClients \
  '{"page_num":1,"page_size":10}' "$RESULT_DIR/list-user-list.json"
signed_call "$LIST_ACCESS_KEY" "$LIST_SECRET_KEY" DescribeClient \
  '{"client_id":"offline-smoke-client"}' "$RESULT_DIR/list-user-describe.json"
signed_call "$GET_ACCESS_KEY" "$GET_SECRET_KEY" DescribeClient \
  '{"client_id":"offline-smoke-client"}' "$RESULT_DIR/get-user-describe.json"
signed_call "$GET_ACCESS_KEY" "$GET_SECRET_KEY" ListClients \
  '{"page_num":1,"page_size":10}' "$RESULT_DIR/get-user-list.json"

assert_status "$RESULT_DIR/list-user-list.json" OK
assert_status "$RESULT_DIR/list-user-describe.json" UNAUTHORIZED
assert_status "$RESULT_DIR/get-user-describe.json" NOT_FOUND
assert_status "$RESULT_DIR/get-user-list.json" UNAUTHORIZED
rg -q "User:rip2-list has no permission to access Admin:proxy.admin.client" \
  "$RESULT_DIR/list-user-describe.json"
rg -q "User:rip2-get has no permission to access Admin:proxy.admin.client" \
  "$RESULT_DIR/get-user-list.json"

cat > "$RESULT_DIR/summary.txt" <<EOF
authenticated-super-list=OK
unsigned-list=UNAUTHORIZED: username cannot be null.
bad-signature-list=UNAUTHORIZED: check signature failed.
rip2-list-list=OK
rip2-list-describe=UNAUTHORIZED
rip2-get-describe=NOT_FOUND
rip2-get-list=UNAUTHORIZED
resource=Admin:proxy.admin.client
EOF

cat "$RESULT_DIR/summary.txt"
echo "authenticated smoke evidence: $RESULT_DIR"
