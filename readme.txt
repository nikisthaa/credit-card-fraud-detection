# Get session token
aws sts get-session-token --duration-seconds 129600


# Format hadoop filesystem
bin/hdfs namenode -format

# Start namenode and datanode
start-dfs.sh

# Stop daemon
stop-dfs.sh

# start yarn
start-yarn.sh

NameNode - http://localhost:9870/

ResourceManager - http://localhost:8088/

# stop yarn
stop-yarn.sh


https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html

https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html

"AccessKeyId": "ASIA2I2VR7W6WVQGTXWI",
        "SecretAccessKey": "ZiYjLPYS+clf/B88Tdc77n70+IBh2wB/POgEe7M+",
        "SessionToken": "FwoGZXIvYXdzEP7//////////wEaDGxzVbZVzEAu/s9fuSJqEix1PfUz4LZV9dFNW1jMDDKBzY6HhNKar8O2yN3NSvb54m/CDCDHevZbLKsYThXinNXsgfcrvm2nIB41hucuHlvP3VYaLgyU5gw2+WhPTRgvqy8sVUKgjhvWKImeJuwL7b54Mxz/VfhUMCib1oScBjIoKa3dYZ9skcOPQzH9jq3HqHJbplMcMXCCKW6E5BCgoac6PhjwnKlfnQ==",
        "Expiration": "2022-11-25T21:52:43Z