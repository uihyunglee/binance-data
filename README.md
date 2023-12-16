# binance-data
Create &amp; Update binance crypto currency data

## 수집 Ticker

> BTC, ETH, XRP, EGLD

collect USDT only. Spot &amp; Futures OHLCV data were collected.

## 수집 가능한 interval

> ['1m', '3m', '5m', '15m' '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '1w']

## 프로젝트 진척 상황 (hongcana)

1. docker 초기 설정

- Python Downgrade to 3.9
- Dockerfile 생성 및 이미지 생성( image name : binance_data:1.0.0 )
- docker-compose.yaml 구성 (postgre, redis, pgAdmin 포함)
- postgreDB 연결을 위한 config.json 및 configDummy 구성


2. airflow DAG 작성

- airflow DAG 작성에 필요한 local python venv의 library 셋업
- 로컬 환경 개발을 위한 .env 추가
- dags_collect_binance_data_5m.py 개발 시작..