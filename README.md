# binance-data
Create &amp; Update binance crypto currency data

## 수집 가능한 interval

> ['1m', '3m', '5m', '15m' '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '1w']

## REQUIREMENTS

- Docker
- Python 3.11

## INSTALLATION

1. 터미널에서 clone한 폴더 경로로 이동한 뒤, 아래 명령어를 실행합니다.
> docker-compose build

이미지 이름 변경을 원하실경우, docker-compose.yaml도 같이 수정해주셔야 합니다.

2. 아래 명령어를 실행하여 컨테이너를 만들고 실행시킵니다.
> docker-compose up

3. https://localhost:8080으로 접속하여 airflow GUI에서 DAGs를 실행시킵니다.

4. 필요에 따라서 http://localhost:5050(pgAdmin 주소)에서 postgreDB를 관리하세요.

## 프로젝트 진척 상황 (hongcana)

1. docker 초기 설정

- Dockerfile 생성 및 이미지 생성( image name : binance_data:1.0.0 )
- docker-compose.yaml 구성 (postgre, redis, pgAdmin 포함)
- postgreDB 연결을 위한 config.json 및 configDummy 구성

2. airflow DAG 작성

- airflow DAG 작성에 필요한 local python venv의 library 셋업
- 로컬 환경 개발을 위한 .env 추가
- dags_collect_binance_data_5m.py 개발 시작