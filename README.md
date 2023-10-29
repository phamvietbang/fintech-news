# fintech-news
Crawl data fintech news using kafka and spark

## Crawl data

```shell
python3 run.py multi_processing_crawler -j baodautu -s 1 -e 10 -k localhost:29092
```

#### run command below to show all the params
```shell
python3 run.py multi_processing_crawler --help
```

## Process data

```shell
python3 run.py multi_processing_exporter -j baodautu -k localhost:29092
```

#### run command below to show all the params
```shell
python3 run.py multi_processing_exporter --help
```
