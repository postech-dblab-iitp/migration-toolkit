[![English](
https://img.shields.io/badge/language-English-orange.svg)](README_EN.md)
[![Korean](
https://img.shields.io/badge/language-Korean-blue.svg)](README.md)

# GDBMS Migration Toolkit for TurboGraph++

MiT (Migration Toolkit)은 관계형 DB를 자동으로 그래프 DB 모델로 변경 후 변경된 모델을 기반으로 관계형 데이터를 그패르 DBMS에 맞게 정점, 간선으로 변경하는 도구이다. MiT는 다양한 관계형 DBMS을 지원하기 위해 CUBRID Migration Toolkit [11.0.0.0002 버전] (https://github.com/cubrid/cubrid-migration)을 기반으로 개발되었다.

MiT의 추가된 기능은 다음과 같다.
 * 그래프 DBMS 연결 및 테스트
 * 메타정보를 기반으로 자동 모델 변경 및 수정 
 * 변경된 모델을 기반으로 다양한 데이터 이관 (on-line, cypher file, csv file)
 * 데이터 이관에 대한 이관 보고서

## 시작하기

MiT는 리눅스 환경에 소스 코드 다운로드 후 빌드 스크립트를 통해 빌드할 수 있다.

## 소스 다운로드

```
git clone https://github.com/postech-dblab-iitp/migration-toolkit.git
```

## 프로그램 빌드

### 빌드 요구 사항

현재 MiT의 빌드는 Linux 환경에서만 지원한다.
Centos 7버전을 권장한다.

빌드에 필요한 java, eclipse는 com.cubrid.cubridmigration.build 내부에 포함되어 있기 때문에 사용자가 추가로 환경을 구축할 필요는 없다.
각 프로그램의 버전은 다음과 같다.

 - java 1.7
 - Eclipse IDE Helios R

### 빌드 실행 방법

```
cd com.cubrid.cubridmigration.build
sh build.sh
```

## 참고 사항

- CUBRID CMT
    - https://github.com/CUBRID/cubrid-migration.git

## 라이센스

MiT는 CUBRID CMT와 동일한 라이센스를 적용받는다.
- Apache license 2.0

## 프로젝트 디렉토리
 
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

## 도움 받기

http://jira.iitp.cubrid.org/secure/Dashboard.jspa

버그, 개선 사항, 질문이 있는 경우 위 jira에 내용을 남기면 지원을 받을 수 있다.