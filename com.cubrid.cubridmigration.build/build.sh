#!/bin/sh

SHELL_DIR=$(dirname $(readlink -f $0))

echo "SHELL_DIR : $SHELL_DIR"

ARG=$@

if [ -z $ARG ]; then
WORKSPACE=$(dirname ${SHELL_DIR})
else
WORKSPACE=${ARG}
fi

JAVA_HOME=${SHELL_DIR}/java
PRODUCT_DIR=cubridmigration
PRODUCT_NAME=CUBRIDMigration
ECLIPSE_HOME=${SHELL_DIR}/eclipse_for_build/eclipse
BUILD_HOME=${WORKSPACE}
echo "BUILD_HOME : ${BUILD_HOME}"
BUILD_DIR=${BUILD_HOME}/com.cubrid.cubridmigration.build
VERSION_DIR=${BUILD_HOME}/com.cubrid.cubridmigration.ui
VERSION_FILE_PATH=${VERSION_DIR}/version.properties
VERSION=`grep buildVersionId ${VERSION_FILE_PATH} | awk 'BEGIN {FS="="} ; {print $2}'`
CUR_VER_DIR=`date +%Y%m%d`
CUR_VER_DIR=cubridmigration-deploy/${CUR_VER_DIR}_${VERSION}
OUTPUT_DIR=${BUILD_HOME}/${CUR_VER_DIR}
MAKENSIS_EXEC_PATH=${SHELL_DIR}/nsis/makensis.exe
MAKENSIS_INPUT_PATH="c:/${PRODUCT_DIR}/com.cubrid.cubridmigration.build/deploy"
MAKENSIS_OUTPUT_PATH="c:/${CUR_VER_DIR}"

cd $WORKSPACE/com.cubrid.cubridmigration.build
if [ ! -d "$WORKSPACE/com.cubrid.cubridmigration.build/lib" ]; then
  wget http://ftp.cubrid.org/CUBRID_Docs/CMT/cmt-build-3rdparty-libs.tgz
  tar xvfpz cmt-build-3rdparty-libs.tgz
fi

if [ ! -f "$WORKSPACE/com.cubrid.cubridmigration.build/java" ]; then
  cat java.tar* | tar xvfz -
fi

cd $WORKSPACE

echo "${PRODUCT_NAME} ${VERSION} build is started..."
echo "OUTPUT PATH is ${OUTPUT_DIR}"

rm -rf ${OUTPUT_DIR}
mkdir -p ${OUTPUT_DIR}
cd ${BUILD_HOME}
${JAVA_HOME}/bin/java -jar ${ECLIPSE_HOME}/plugins/org.eclipse.equinox.launcher_1.1.0.v20100507.jar -application org.eclipse.ant.core.antRunner -buildfile ${BUILD_DIR}/buildProduct.xml -Doutput.path=${OUTPUT_DIR} -Declipse.home=${ECLIPSE_HOME} -Dmakensis.path=${MAKENSIS_EXEC_PATH} -Dmakensis.input.path=${MAKENSIS_INPUT_PATH} -Dmakensis.output.path=${MAKENSIS_OUTPUT_PATH} -Dproduct.version=${VERSION} distlinux
${JAVA_HOME}/bin/java -jar ${ECLIPSE_HOME}/plugins/org.eclipse.equinox.launcher_1.1.0.v20100507.jar -application org.eclipse.ant.core.antRunner -buildfile ${BUILD_DIR}/buildPlugin.xml -Doutput.path=${OUTPUT_DIR} -Declipse.home=${ECLIPSE_HOME} -Dmakensis.path=${MAKENSIS_EXEC_PATH} -Dmakensis.input.path=${MAKENSIS_INPUT_PATH} -Dmakensis.output.path=${MAKENSIS_OUTPUT_PATH} -Dproduct.version=${VERSION} distlinux
