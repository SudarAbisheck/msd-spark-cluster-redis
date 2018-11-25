# MSD: Apache Spark application with Redis as a Datastore


## Setting up the docker containers

1. #### Creating a common datstore volume for Spark Cluster
`docker volume create --name spark-datastore`

2. #### Copying necessary files to the datastore volume

`unzip spark-datastore/data.csv.zip -d spark-datastore/`

`sudo cp spark-datastore/* $(docker inspect -f {{.Mountpoint}} spark-datastore)`

3. #### Creating a Bridge Network for the application
`docker network create msd_network`

4. #### Building all the images
```
docker build -t sudar/spark-master ./spark-master
docker build -t sudar/spark-worker ./spark-worker
docker build -t sudar/flask-server ./flask-server
docker pull redis:latest
```

5. #### Starting the Spark Master Node
`docker run -d -v spark-datastore:/data --net msd_network -p 8080:8080 -p 7077:7077 --name spark-master sudar/spark-master `

6. #### Starting the Spark Worker nodes

Any number of workers can be added. I chose to launch one worker.

`docker run -d -v spark-datastore:/data --net msd_network --name spark-worker sudar/spark-worker`

7. #### Starting Redis Cluster (6 nodes)
```
for ind in `seq 1 6`; do \
 docker run -d \
 -v $PWD/redis/cluster-config.conf:/usr/local/etc/redis/redis.conf \
 --name "redis-$ind" \
 --net msd_network \
 --publish "700$ind:6379" \
 redis redis-server /usr/local/etc/redis/redis.conf; \
done

redis-cli --cluster create \
    $(for ind in `seq 1 6`; do \
        echo -n $(docker inspect -f '{{(index .NetworkSettings.Networks "msd_network").IPAddress}}' redis-$ind)':6379 '; \
    done
    ) \
--cluster-replicas 1
```

8. #### Starting the Flask server (Loads the data from csv and submits the job to Spark)

`docker run -d -v spark-datastore:/data --net msd_network -p 5000:5000 --name flask-server sudar/flask-server`

To check the flask server logs:

`docker container logs -f $(docker ps -a -q --filter "name=flask-server")`


## Testing the Application

1. `/getRecentItem` - return the most recent item added on the given date.

Sample Output: 
```
$ curl http://localhost:5000/getRecentItem/2016-05-31
{"colors": ["Black Patent", "Clearwhite", "ClearBlack", "Clear and White"], "brand": "Pleaser", "id": "AVpf0c_lLJeJML43EU2D"}
```

2. `/getBrandsCount` - return the count of each brands added on the given date in descending order.

Sample Output: 
```
$ curl http://localhost:5000/getBrandsCount/2016-05-31
[["GUESS", 2], ["Nine West", 2], ["Style & Co.", 1], ["Christian Louboutin", 1], ["Burberry", 1], ["Gucci", 1], ["JAMBU", 1], ["Sweet Ballerina", 1], ["Michael Kors", 1], ["Skechers", 1], ["VANELi", 1], ["Josmo", 1], ["Alfani", 1], ["Mossimo", 1], ["Clarks Artisan Collection", 1], ["Nike", 1], ["eastory", 1], ["Bandolino", 1], ["Coach", 1], ["Pleaser", 1], ["Reef", 1], ["Aerosoles", 1], ["Naot Footwear", 1], ["Abound", 1], ["Rampage", 1], ["Khombu", 1], ["Jessica Simpson", 1], ["Softspots", 1], ["DDI", 1], ["Very Fine", 1], ["Clarks Artisan", 1], ["Robert Clergerie", 1], ["Sebago", 1]]

```

3. `/getItemsbyColor` - return the top 10 latest items given input as color.

Sample Output: 
```
$ curl http://18.207.116.141:5000/getItemsbyColor/Blue
[{"brand": "Novica", "dateAdded": "2017-03-28T11:55:43.000Z", "colors": ["Blue"], "id": "AVsUxZClnnc1JgDc4Mev"}, {"brand": "Skechers", "dateAdded": "2017-03-28T11:45:23.000Z", "colors": ["Blue"], "id": "AVsUvBi-v8e3D1O-mE3n"}, {"brand": "Journee Collection", "dateAdded": "2017-03-28T11:45:01.000Z", "colors": ["Beige", "Black", "Blue", "Grey", "Red"], "id": "AVsUu8Wqnnc1JgDc4L4b"}, {"brand": "PINUP COUTURE", "dateAdded": "2017-03-28T11:44:54.000Z", "colors": ["Black", "Blue", "Red", "White"], "id": "AVsUu6fOnnc1JgDc4L3x"}, {"brand": "C Label", "dateAdded": "2017-03-28T11:44:31.000Z", "colors": ["Blue", "Gold", "Red", "Silver"], "id": "AVsUu02GQMlgsOJE7HBg"}, {"brand": "Under Armour", "dateAdded": "2017-03-28T11:43:59.000Z", "colors": ["Blue"], "id": "AVsUutCvv8e3D1O-mEtu"}, {"brand": "Fahrenheit", "dateAdded": "2017-03-28T11:43:15.000Z", "colors": ["Blue", "Gold", "Grey", "Silver"], "id": "AVsUuiYjU2_QcyX9Pa7r"}, {"brand": "Bamboo", "dateAdded": "2017-03-28T11:43:06.000Z", "colors": ["Black", "Blue", "Tan"], "id": "AVsUugQuv8e3D1O-mEof"}, {"brand": "Sperry", "dateAdded": "2017-03-28T11:42:39.000Z", "colors": ["Blue"], "id": "AVsUuZsuQMlgsOJE7G0P"}, {"brand": "White Mountain", "dateAdded": "2017-03-28T11:42:27.000Z", "colors": ["Blue"], "id": "AVsUuWxinnc1JgDc4Ln3"}]

```


### Notes

Multiple Recent Items for a given date. So decided to return only one when making a request.

|AVsRobK8QMlgsOJE61dD|        Big Buddha|         BLACK,TAUPE|2017-03-27 21:17:41|   1|
|AVsRobNlQMlgsOJE61dE|        Big Buddha|              COGNAC|2017-03-27 21:17:41|   1|


Also, had to manually build the [spark-redis](https://github.com/RedisLabs/spark-redis) library, as only the very recent version had support for python (dataframes). The built JAR file is placed under the `spark-datastore` directory.

### Appendix (Command StratchPad)

```
# To sync local files to an EC2 instance.
watchman-make  -p "**/*" --run='rsync -r -a --delete -v -e "ssh -i ~/keys/pubkey" ~/msd ubuntu@18.207.116.200:~/'

spark-shell --master spark://localhost:7077 --packages com.databricks:spark-csv_2.11:1.5.0,com.redislabs:spark-redis:2.3.0

docker build -t sudar/flask-server ./flask-server && \
docker run -d -v spark-datastore:/data --net msd_network -p 5000:5000 --name flask-server sudar/flask-server && \
docker container logs -f $(docker ps -a -q --filter "name=flask-server")

docker stop flask-server && docker rm $(docker ps -a -q --filter "name=flask-server")

for ind in `seq 1 6`; do docker stop redis-$ind && docker rm $(docker ps -a -q --filter "name=redis-$ind"); done

docker container logs -f $(docker ps -a -q --filter "name=flask-server")

docker exec -it spark-master /bin/bash

spark-submit --master spark://localhost:7077 ./datastore/app.py

docker inspect -f '{{(index .NetworkSettings.Networks "msd_network").IPAddress}}' redis-1

docker run -d -v spark-datastore:/data --net msd_network -p 5000:5000 sudar/flask-server

curl http://localhost:5000
curl http://localhost:5000/getRecentItem/2016-05-31
curl http://localhost:5000/getBrandsCount/2016-05-31
curl http://localhost:5000/getItemsbyColor/Blue

redis-cli -c -h localhost -p 7001

docker exec redis-1 redis-cli cluster nodes

docker stop spark-worker && docker rm $(docker ps -a -q --filter "name=spark-worker")
```