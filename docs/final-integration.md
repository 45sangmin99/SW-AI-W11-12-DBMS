# 통합 엔진과 SQL 사용법

## 반영 범위

- Sangmin의 고정 길이 바이너리 슬롯, COW 스냅샷, 64KiB WAL 버퍼링, GROUP BY, AS, CREATE TABLE, UNIQUE 인덱스를 기반으로 합니다.
- Hyeonju의 대화형 콘솔을 통합했습니다. `.tables`와 `.schema`는 파일 목록 대신 엔진 카탈로그를 읽으므로 CREATE TABLE로 추가한 테이블도 표시합니다.
- Nuri의 한정 상품 주문 데모를 통합했습니다. 기본 키 의존성을 커밋 시 비교하는 OCC 구현 대신 잠금 방식을 적용했습니다.

## 동시성 제어

일반 SELECT는 BEGIN 시점의 스냅샷과 자신의 미커밋 변경을 읽습니다. 다른 트랜잭션의 쓰기를 막지 않습니다. UPDATE/DELETE와 SELECT … FOR UPDATE는 대상 잠금을 먼저 확보하고, 최신 커밋 상태와 자신의 변경을 읽습니다. 일반 조회가 읽은 값을 나중에 계산해 수정할 경우에는 처음부터 FOR UPDATE를 사용해야 합니다. 일반 SELECT에는 읽은 값의 변경 여부를 나중에 검사하는 OCC 검사가 없습니다.

```sql
BEGIN;
SELECT stock FROM drops WHERE id=1 FOR UPDATE;
UPDATE drops SET stock=2 WHERE id=1;
INSERT INTO orders VALUES (1);
COMMIT;
```

- `id = 정수`를 안전하게 추출할 수 있으면 행 단위 배타 잠금입니다. 없는 ID도 잠가 해당 ID의 자동 삽입과 충돌합니다.
- 그 밖의 조건·전체 조회에 FOR UPDATE를 붙이거나 범위 UPDATE/DELETE를 실행하면 테이블 전체를 잠급니다. 범위 잠금보다 보수적인 구현입니다.
- INSERT는 테이블 전체 잠금과 충돌하는 의도 잠금, 예약한 자동 ID의 행 잠금을 사용합니다. 서로 다른 ID의 INSERT는 함께 준비할 수 있습니다.
- 잠금은 COMMIT·ROLLBACK·세션 종료/만료까지 유지됩니다. 실패한 명시적 트랜잭션도 ROLLBACK 또는 세션 종료가 필요합니다.
- 기다리는 동안 엔진 mutex는 해제합니다. 잠금 대기는 한 문장당 약 1초이며 시간이 지나면 ERR_BUSY, HTTP 409를 반환하고 트랜잭션을 FAILED로 표시합니다. 교착 상태는 이 타임아웃으로 해소하며 별도 wait-for 그래프는 없습니다. 호출자는 ROLLBACK하여 보유 잠금을 해제해야 합니다.
- 서로 다른 행의 변경은 커밋 시 최신 스냅샷에 합칩니다. UNIQUE 제약도 이 결과를 기준으로 검사합니다. WAL 기록과 실제 데이터 반영은 기존 커밋 mutex로 직렬 처리합니다.
- HTTP SQL 작업자 수가 적고 잠금 대기가 작업자를 모두 차지하면 COMMIT 요청도 큐에서 기다릴 수 있습니다. 무제한 정지가 되지는 않지만 잠금 타임아웃이 발생할 수 있습니다. 비동기 잠금 스케줄러·기아 방지 FIFO는 미구현입니다.
- 트랜잭션당 잠금 수는 `MDB_TX_MAX_LOCKS`(기본 2,000,000)로 제한합니다. 행 잠금은 확장 가능한 해시 인덱스로 찾습니다. 테이블 전체 잠금은 기존 잠금을 순회하며, 세션 종료 시에는 보유 잠금을 목록에서 정리합니다.

일반 스냅샷 조회를 유지하는 방식이며 모든 SELECT에 공유 잠금을 거는 strict 2PL 또는 직렬화 가능 격리를 구현한 것은 아닙니다. 여러 행의 업무 제약을 보호하려면 관련 행을 모두 FOR UPDATE로 잠그거나 테이블 잠금으로 처리해야 합니다.

## 집계와 기간 조회

저장 열은 INT와 VARCHAR입니다. SUM/AVG는 INT, MIN/MAX는 INT와 VARCHAR를 지원합니다. COUNT는 COUNT(*) 형식입니다. 저장 열의 NULL은 지원하지 않지만 빈 입력의 SUM/AVG/MIN/MAX는 결과 NULL을 반환합니다. COUNT(*)는 0입니다. SUM의 int64 중간 합이 넘치면 오류로 처리합니다. AVG는 근사 실수이며 결과 열의 형식은 REAL입니다. HTTP JSON에서는 기존 INT와 같이 수치를 문자열로 전달하고 NULL만 JSON null로 전달합니다.

```sql
SELECT SUM(amount) AS total, AVG(amount) AS mean,
       MIN(amount) AS smallest, MAX(amount) AS largest
FROM events WHERE created>=1704067200 AND created<1706745600;

SELECT MONTH(created) AS month, SUM(amount) AS total, COUNT(*) AS n
FROM events GROUP BY MONTH(created) ORDER BY month;
```

MONTH는 Unix epoch **초** INT를 UTC의 YYYY-MM으로 바꿔 묶습니다. 0001~9999년 범위이며 운영체제 time_t가 표현하지 못하는 시각은 오류입니다. 밀리초·문자열 날짜·시간대·DATE_TRUNC는 지원하지 않습니다. MONTH는 현재 GROUP BY 표현식과 그 출력에 사용합니다. 확장 집계의 ORDER BY는 출력한 표현식 또는 AS 별칭을 사용합니다. GROUP BY에서 AS 별칭은 지원하지 않습니다. 그룹별 작업 공간은 `MDB_MAX_GROUPS`로 제한합니다.

기간 WHERE는 기존 정수 비교로 처리합니다. 급증 판정은 기간별 집계 결과를 애플리케이션에서 비교해야 합니다. 윈도 함수, HAVING, 하위 질의는 구현하지 않았습니다.

## 테이블 결합

`JOIN`과 `INNER JOIN`은 같은 자료형의 열을 비교해 두 테이블을 결합합니다. 일치하는 행의 쌍마다 결과가 하나씩 생성됩니다. 두 테이블에 같은 열 이름이 있으면 테이블 이름이나 별칭으로 구분합니다.

```sql
SELECT c.name AS character_name, i.name AS item_name
FROM characters AS c
INNER JOIN items AS i ON c.attack = i.price
WHERE i.price >= 10
ORDER BY c.id, i.id;
```

두 입력은 같은 트랜잭션 스냅샷과 세션의 아직 커밋하지 않은 변경을 읽습니다. 기존 `WHERE`, `GROUP BY`, 집계, `ORDER BY`, `LIMIT`과 `OFFSET`을 함께 사용할 수 있습니다. 자기 조인에는 서로 다른 별칭을 사용합니다. 입력 열은 두 테이블을 합쳐 최대 32개이며, 오른쪽 입력의 정렬 작업 공간은 `MDB_MAX_SORT_ROWS` 행으로 제한합니다. 지원 범위는 단일 동등 조건의 내부 조인입니다.

## 페이지 조회

한 응답은 계속 최대 10,000행입니다. LIMIT에 OFFSET을 붙여 다음 구간을 조회할 수 있습니다. OFFSET은 음수가 아닌 정수이며 LIMIT이 필요합니다.

```sql
BEGIN;
SELECT id,value FROM pages ORDER BY id LIMIT 10000 OFFSET 0;
SELECT id,value FROM pages ORDER BY id LIMIT 10000 OFFSET 10000;
COMMIT;
```

여러 페이지를 같은 읽기 트랜잭션에서 조회하면 중간에 다른 트랜잭션이 변경해도 같은 스냅샷을 읽습니다. 안정적인 순서는 `ORDER BY id`처럼 유일한 열로 지정해야 합니다. OFFSET이 크면 앞선 행을 다시 읽는 비용이 생기며 ORDER BY는 `MDB_MAX_SORT_ROWS`(기본 1,000,000)의 작업 공간 제한도 받습니다. 인덱스 범위 커서·서버 커서·스트리밍 응답은 미구현입니다. 모든 행을 한 번의 응답에 실어 보내도록 한도를 해제한 구현은 아닙니다.

## 빌드와 검증

```sh
make -C reliable
make -C reliable test test-crash test-console test-final test-demo
ASAN_OPTIONS=detect_leaks=0 make -C reliable BUILD=build/final-asan \
  CFLAGS='-std=c11 -O1 -g -Wall -Wextra -Werror -pedantic -fsanitize=address,undefined -fno-omit-frame-pointer' \
  test test-crash test-console test-final test-demo
```

`test-final`은 엔진의 대기 지점 후크를 이용한 잠금 검증과 실제 HTTP의 집계·월별 집계·100,000행 10페이지 조회를 실행합니다. `test-demo`는 Node.js가 필요합니다. 빌드 디렉터리를 바꾸면 위 명령에 같은 BUILD 값을 지정합니다.

검증 환경은 macOS ARM64 / Apple clang / C11입니다. Linux·WSL2 실행과 실제 전원 차단은 이 변경에서 수행하지 않았습니다. 기존 crash 검사는 실제 SIGKILL과 반복 복구를 검사하며 전원 장애 자체를 재현하지 않습니다. 새 잠금 방식과 PostgreSQL의 성능 비교는 수행하지 않았습니다.

## 이번 통합의 실행 결과

Apple M2, Darwin 25.5.0(ARM64), Apple clang 21.0.0, Python 3.9.6, Node.js 24.16.0에서 확인했습니다. 일반 빌드는 `-O2`, sanitizer 빌드는 `-O1`과 AddressSanitizer/UndefinedBehaviorSanitizer를 사용했습니다.

| 검사 | 결과 |
|---|---|
| `make ... test` | SQL 30,000개 퍼즈 입력, B+ 트리·슬롯·그룹 해시, 엔진, HTTP, 큐·수명 주기, DDL·UNIQUE, 손상·I/O 주입·참조 모델 통과 |
| `test-crash` | 실제 SIGKILL, 다중 테이블 복구, 커밋 응답 후 복구, 반복 복구 통과 |
| `test-console` | 25/25 통과. WAL 카탈로그의 새 테이블과 재시작 후 `.schema`, AVG/NULL 출력 포함 |
| `test-final` | 엔진 잠금 대기 후크, 없는 ID와 INSERT 충돌, 교착 타임아웃·롤백, HTTP 스냅샷/최신 읽기·독립 행 커밋 통과 |
| `test_analytics.py` | 21개 시드 기반 그룹 집계 조건을 Python 결과와 대조, 소수 AVG·UTF-8 키·UTC 경계·빈 집계·작업 공간 오류 통과 |
| 페이지 조회 | 실제 HTTP로 같은 스냅샷의 100,000행을 10,000행씩 10회 조회. 누락·중복 없음 |
| `test-demo` | 정적 자산 HTTP 제공, 마지막 재고 동시 주문, 품절, 재시작 후 재고 0/주문 3건 통과 |
| ASan/UBSan | 위 Make 타깃 모두 통과. macOS에서 `detect_leaks=0`으로 실행했으므로 누수 검사를 수행했다고 주장하지 않음 |
| 선택 DOM 검사 | 기존 Mini React 저장소의 jsdom을 사용해 mount, 클릭 이벤트, 재고 갱신 3회, 주문 목록, 품절 버튼 통과. 실제 브라우저 시각 검사와 구분 |

실행 로그는 로컬 `build/final-tests.log`, `build/final-asan.log`에 있습니다. 빌드 산출물과 로그는 Git에서 제외합니다. 변경 전 OCC의 WSL 측정값을 현재 엔진의 측정값으로 옮기지 않았습니다.

스냅샷 조회와 비교용 2PL의 실측은 [동시성 제어 성능 비교](isolation-comparison.md)에 별도로 정리했습니다.
