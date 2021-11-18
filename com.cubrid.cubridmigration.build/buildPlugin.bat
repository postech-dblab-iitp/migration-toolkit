rem @echo off

set ECLIPSE_HOME=D:\dailybuild\eclipse
set BUILD_HOME=%CD%
set BUILD_DIR=%BUILD_HOME%\trunk\com.cubrid.cubridmigration.build

rd /s/q %BUILD_HOME%\trunk

rem set svn information
set SVN_USER=anonsvn
set SVN_PWD=anonsvn

set CMT_SVN_URL=https://svn.cubrid.org/cubridtools/cubrid-migration-toolkit/trunk
svn co --username %SVN_USER% --password %SVN_PWD% %CMT_SVN_URL%

set CMT_SVN_URL=https://svn.cubrid.org/cubridtools/cubrid-manager/trunk/com.cubrid.common.configuration
svn co --username %SVN_USER% --password %SVN_PWD% %CMT_SVN_URL%

rem make current version output directory
set CUR_VER_DIR=%DATE%
set CUR_VER_DIR=%CUR_VER_DIR:~0,10%
set CUR_VER_DIR=%CUR_VER_DIR:/=%
set CUR_VER_DIR=%CUR_VER_DIR:-=%

rd /s/q %BUILD_HOME%\%CUR_VER_DIR%
md %BUILD_HOME%\%CUR_VER_DIR%
set OUTPUT_DIR=%BUILD_HOME%\%CUR_VER_DIR%

java -jar %ECLIPSE_HOME%\plugins\org.eclipse.equinox.launcher_1.1.0.v20100507.jar -application org.eclipse.ant.core.antRunner -buildfile %BUILD_DIR%\buildPlugin.xml -Doutput.path=%OUTPUT_DIR% -Declipse.home=%ECLIPSE_HOME% distwin