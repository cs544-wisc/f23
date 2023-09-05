echo "-Xms128M" >> /apache-cassandra-4.0.7/conf/jvm-server.options
echo "-Xmx128M" >> /apache-cassandra-4.0.7/conf/jvm-server.options

sed -i "s/^listen_address:.*/listen_address: "`hostname`"/" /apache-cassandra-4.0.7/conf/cassandra.yaml
sed -i "s/^rpc_address:.*/rpc_address: "`hostname`"/" /apache-cassandra-4.0.7/conf/cassandra.yaml
sed -i "s/- seeds:.*/- seeds: p6_db_1,p6_db_2,p6_db_3/" /apache-cassandra-4.0.7/conf/cassandra.yaml

/apache-cassandra-4.0.7/bin/cassandra -R

sleep infinity