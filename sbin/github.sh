curl -k https://github.com/alibaba/RocketMQ/issues/1 > z

grep -o '\b[0-9a-zA-Z_.\-]\+@[0-9a-zA-Z_]\+\.[0-9a-zA-Z_]\+\b' z |uniq > z1

echo "RocketMQ github user=========================="
cat z1
