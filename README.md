# fintech-news
Crawl data fintech news using kafka and spark

## Set up
Create file .env like file [.env.example](.env.example) and the setting features in [configs.py](configs.py)

## Run servers

### Run kafka clusters
```shell
docker compose -f kafka-compose.yaml up -d
```

### Run elastic-kibana clusters
```shell
docker compose -f elasticsearch-kibana-compose.yaml up -d
```

### Run mongodb
```shell
docker pull mongo:latest
docker run -d -p 27017:27017 --name=mongodb mongo:latest
```

## Crawl data

```shell
python3 run.py multi_processing_crawler -j vietnamnet -s 1 -e 10 -u localhost:29092
```

#### run command below to show all the params
```shell
python3 run.py multi_processing_crawler --help
```

## Process data

```shell
python3 run.py multi_processing_exporter -j vietnamnet -u localhost:29092
```

#### run command below to show all the params
```shell
python3 run.py multi_processing_exporter --help
```
