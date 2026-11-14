# AFTERIMAGE Limited Drop

한정판 포스터 세 장을 판매하는 로컬 데모입니다. 브라우저에서 상품과 남은 수량을 보고 한 장씩 주문할 수 있습니다. 최근 확정 주문 세 건을 표시합니다.

## 실행

DBMS 저장소 루트에서 실행하세요. C 서버에는 Linux·macOS 또는 WSL이 필요하고, 웹 서버에는 Node.js 20 이상이 필요합니다. 형제 폴더 `SW-AI-W11-12-REACT`에서 Mini React 엔진 모듈을 직접 제공합니다.

터미널 1에서 새 데모 DB를 준비하고 실행합니다.

```sh
make -C reliable
demo_dir=$(mktemp -d)
cp examples/limited-drop/data/* "$demo_dir/"
echo "$demo_dir"
MINIDB_TOKEN=limited-drop-demo ./reliable/build/mini-db-server --data-dir "$demo_dir" --port 8080 --threads 4
```

터미널 2에서 웹 서버를 시작합니다. Windows에서 Node.js를 쓴다면 PowerShell 명령을 사용하세요.

```sh
MINIDB_TOKEN=limited-drop-demo node examples/limited-drop/server.mjs
```

```powershell
$env:MINIDB_TOKEN = 'limited-drop-demo'
$env:DROP_PORT = '13000'
node examples/limited-drop/server.mjs
```

브라우저에서 [상품 페이지](http://127.0.0.1:13000/)를 엽니다. `EACCES` 또는 `EADDRINUSE`가 나오면 `DROP_PORT`를 다른 포트(예: `13001`)로 지정해 다시 실행하고, 터미널에 표시된 주소를 엽니다. DBMS 주소는 `MINIDB_URL`로 바꿀 수 있습니다.

재시작 후 확인할 때는 앞서 출력된 데이터 경로로 DBMS만 다시 시작하고 화면의 **재고 및 주문 다시 조회**를 누르세요.

## 엔진을 어떻게 사용했는가

- 한 세션에서 재고를 조회하고 재고 차감과 주문 추가를 준비한 뒤 커밋합니다. 같은 시작 버전의 쓰기가 겹치면 먼저 확정한 쪽의 변경이 유지됩니다. 뒤 요청이 최신 버전에서 시작했다면 재고 0을 읽고 품절로 끝납니다.
- 재고는 기본 키 `id = 1`로 조회해 DBMS의 B+ 트리 경로를 사용합니다. 재고와 최근 주문은 같은 읽기 세션에서 조회해 화면에 함께 반영합니다.
- Mini React가 재고 텍스트, 주문 버튼, 상태 문구, 주문 목록을 갱신합니다. 목록의 `key`는 DBMS가 문자열로 돌려주는 주문 ID입니다. 엔진은 상태를 갱신할 때 새 Virtual DOM을 계산하고, diff 결과만 실제 DOM에 적용합니다.
- Node 웹 서버는 정적 파일 제공과 두 개의 업무 API를 담당합니다. DB 토큰과 SQL은 서버에 둡니다. 데모의 상품은 한 종류이며 두 서버는 로컬에서 실행합니다.

## 한계

- 동시 쓰기 충돌은 DB 전체 버전 기준입니다. 서로 다른 상품을 추가해도 동시에 시작한 쓰기가 충돌할 수 있습니다.
- DBMS의 UPDATE는 기존 계산식 대신 리터럴 대입을 지원합니다. 웹 서버는 같은 트랜잭션에서 재고를 읽고 `BigInt`로 차감량을 계산합니다. CSV 재작성과 인덱스 재구축 비용은 그대로 발생합니다.
- 커밋 결과가 불명확하거나 주문 응답이 끊기면 UI는 재주문을 막고 조회를 제공합니다. 주문 ID를 받은 경우 그 ID가 이력에 나타나면 확정으로 표시합니다. 응답 전체가 유실돼 ID를 받지 못하면 이 화면에서 특정 주문을 판별할 수 없습니다. 결제, 로그인, 주문 중복 제거는 데모 범위 밖입니다.

## 검증

새 데모 DB를 띄운 뒤 같은 접속 환경에서 `node examples/limited-drop/verify.mjs`를 실행하면 HTML과 엔진 모듈 제공, 두 건의 확정 주문, 마지막 한 장의 동시 주문, 최종 품절 상태를 실제 HTTP로 검사합니다. 이 검사는 데모 재고 세 장을 모두 사용합니다.
