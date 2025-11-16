@echo off
docker cp flightDelays.py namenode:/data
docker cp utils.py namenode:/data
docker cp data/full_data_flightdelay.csv namenode:/data
docker exec -it namenode bash -c "hdfs dfs -mkdir /dir"
docker exec -it namenode bash -c "hdfs dfs -mkdir /dir/FlightDelays"
docker exec -it namenode bash -c "hdfs dfs -rm -r /dir/FlightDelays/flightDelays.py"
docker exec -it namenode bash -c "hdfs dfs -rm -r /dir/FlightDelays/utils.py"
docker exec -it namenode bash -c "hdfs dfs -rm -r /dir/full_data_flightdelay.csv"
docker exec -it namenode bash -c "hdfs dfs -put /data/flightDelays.py /dir/FlightDelays"
docker exec -it namenode bash -c "hdfs dfs -put /data/utils.py /dir/FlightDelays"
docker exec -it namenode bash -c "hdfs dfs -put /data/full_data_flightdelay.csv /dir"