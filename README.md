# binance-data
Create &amp; Update binance crypto currency data

## 수집 Ticker

> BTC, ETH, XRP (Spot, Futures)

collect USDT only

## 수집한 interval

> ['1m', '3m', '5m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '1w']

### 프로젝트 진척 상황 (hongcana)

1. docker 초기 설정

- docker-compose.yaml 구성 ( airflow, postgre, redis, pgAdmin)
- postgreDB 연결을 위한 config.json 및 configDummy 구성
- airflow DAG 작성에 필요한 python venv library setup

2. airflow DAG 작성

- 