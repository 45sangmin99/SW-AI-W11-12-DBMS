# AFTERIMAGE Limited Drop

포스터 세 장을 판매하는 로컬 데모입니다. Mini React 화면 → Node.js 업무 API → Mini DBMS HTTP 세션으로 연결합니다. Node.js 표준 라이브러리만 사용하며 화면 엔진은 `engine/`에 포함했습니다. 출처는 [SOURCE.md](engine/SOURCE.md)에 기록했습니다.

## 실행

저장소 루트에서 실행합니다. C11 컴파일러, Make, Python 3, Node.js가 필요합니다.

```sh
make -C reliable

demo_dir=$(mktemp -d)
cp examples/limited-drop/data/* "$demo_dir/"
MINIDB_TOKEN=limited-drop-demo ./reliable/build/mini-db-server --data-dir "$demo_dir" --port 8080 --threads 8
```

다른 터미널에서 실행하고 http://127.0.0.1:13000/ 에 접속합니다.

```sh
MINIDB_TOKEN=limited-drop-demo node examples/limited-drop/server.mjs
```

DB 주소는 `MINIDB_URL`, 웹 포트는 `DROP_PORT`로 변경합니다. DB 디렉터리는 첫 터미널의 `$demo_dir`이며, 재시작할 때 같은 경로를 사용하면 주문이 복구됩니다.

## 주문 처리

1. HTTP 세션에서 `BEGIN`을 실행합니다.
2. `SELECT stock FROM drops WHERE id=1 FOR UPDATE`로 재고 행의 잠금을 확보하고 최신 수량을 읽습니다.
3. 수량이 있으면 차감한 값으로 UPDATE하고 주문을 INSERT합니다.
4. COMMIT 후 세션을 닫습니다. 수량이 없거나 오류가 발생해도 세션 종료로 남은 변경과 잠금을 정리합니다.

같은 재고를 다루는 주문은 잠금을 기다립니다. 이전 주문이 커밋하면 다음 주문이 변경된 수량을 읽습니다. 화면 갱신용 일반 조회는 잠금을 잡지 않고 같은 트랜잭션의 스냅샷으로 재고와 최근 주문을 읽습니다.

`DURABLE` 응답은 WAL 동기화가 완료됐음을 뜻합니다. 응답 유실로 결과를 알 수 없으면 자동으로 재주문하지 않고 이력을 확인하도록 안내합니다. 요청 ID 기반 중복 주문 방지는 구현하지 않았으며 결제·고객 인증도 범위 밖입니다. 잠금 대기는 최대 약 1초 후 실패할 수 있습니다.

## 검증

```sh
make -C reliable test-demo
```

새 임시 DB와 실제 HTTP 요청으로 정적 파일 제공, 두 건의 주문, 마지막 한 장의 동시 주문, 품절과 최종 주문 수를 확인합니다. 브라우저의 시각적 렌더링 검사는 포함하지 않습니다.

기존 브랜치 문서의 WSL2 수치는 기본 키 의존성 검사 방식(OCC)의 측정값입니다. 현재 잠금 방식의 성능 측정값으로 사용하지 않습니다.

선택적으로 이미 설치된 jsdom으로 화면의 DOM 동작을 검사할 수 있습니다. 새 의존성을 자동으로 설치하지는 않습니다.

```sh
node examples/limited-drop/verify-dom.mjs /absolute/path/to/jsdom/lib/api.js
```

이번 환경에서는 인접 Mini React 저장소의 `node_modules/jsdom/lib/api.js`를 사용해 최초 화면, 실제 클릭 이벤트, 세 번의 주문 상태 갱신과 품절 버튼을 확인했습니다. 실제 브라우저의 시각적 배치 검사는 별도입니다.
