[![English](
https://img.shields.io/badge/language-English-orange.svg)](README_EN.md)
[![Korean](
https://img.shields.io/badge/language-Korean-blue.svg)](README.md)

## GDBMS Migration Toolkit for TurboGraph++

IITP-차세대 DBMS 과제 중 RDB에서 TurboGraph++로 이관하는 도구를 개발하기 위하여 CUBRID Migration Toolkit [11.0.0.0002 버전] (https://github.com/cubrid/cubrid-migration) 을 Clone하였다

## 시작하기

IITP RDB to TurboGraph++ 이관 도구 (이하 MiT)의 소스는 해당 github에서 다운로드 할 수 있으며 빌드에 사용되는 스크립트도 포함되어 있다.

### 소스 획득

```
git clone https://github.com/postech-dblab-iitp/migration-toolkit.git
```

### 프로그램 빌드

빌드는 리눅스 환경에서 진행해야 한다.
윈도우는 현재 지원하지 않는다.

```
cd com.cubrid.cubridmigration.build
sh build.sh
```

### 프로젝트 디렉토리
 
- LICENSE/ : MiT에 사용된 라이브러리, 프레임워크 등 의 라이센스가 적혀있는 txt파일 디렉토리
- MiT_Manual/ : MiT의 사용 방법 메뉴얼이 rst 파일로 작성되어 있는 디렉토리
- MiT_docs/ : MiT 설계 관련 문서가 담겨있는 디렉토리
- com.cubrid.common.configuration/ : MiT 프로그램의 실행, 중단, 클래스로드 등 을 관리
- com.cubrid.common.update.feature/ : 업데이트 할 라이브러리와 플러그인 정보
- com.cubrid.common.update/ : MiT의 업데이트 및 업데이트 확인
- com.cubrid.cubridmigration.app.feature/ : app의 feature를 설정
- com.cubrid.cubridmigration.app.update.site/ : MiT 실행 시 표시되는 웹 정보를 가져오는 URL을 저장
- com.cubrid.cubridmigration.app/ : 프로그램 실행 시 표시되는 첫 애플리케이션 화면
- com.cubrid.cubridmigration.build/ : 프로그램을 빌드하는 정보와 쉘 스크립트가 포함된 프로젝트
- com.cubrid.cubridmigration.command/ : 스크립트 이관을 담당하는 프로젝트
- com.cubrid.cubridmigration.core.testfragment/ : core 프로젝트의 테스트 코드
- com.cubrid.cubridmigration.core/ : MiT의 이관, 페이지 이동 등 핵심 기능을 담당
- com.cubrid.cubridmigration.plugin.feature/ : plugin 프로젝트의 feature를 설정
- com.cubrid.cubridmigration.plugin.update.site/ : plugin 업데이트시에 연결할 URL을 저장
- com.cubrid.cubridmigration.plugin/ : MiT의 플러그인을 설정하는 프로젝트
- com.cubrid.cubridmigration.ui.testfragment/ : UI 프로젝트의 테스트 코드
- com.cubrid.cubridmigration.ui/ : MiT의 UI를 담당하는 프로젝트
