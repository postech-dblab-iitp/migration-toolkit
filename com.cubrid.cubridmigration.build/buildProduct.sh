#!/bin/bash

TOP_DIR=${1}
if [ -z "$TOP_DIR" ]; 
then
  BUILD_HOME=`pwd`
else
  BUILD_HOME=`cd ${TOP_DIR} && pwd`
fi

rm -rf ${BUILD_HOME}/trunk

ECLIPSE_HOME=/home/cmt/dailybuild/eclipse
PRODUCT_NAME=cubridmigration
BUILD_DIR=${BUILD_HOME}/trunk/com.cubrid.cubridmigration.build

SVN_USER=anonsvn
SVN_PWD=anonsvn
CM_SVN_URL=https://svn.cubrid.org/cubridtools/cubrid-migration-toolkit/trunk

svn export --username ${SVN_USER} --password ${SVN_PWD} ${CM_SVN_URL}

#make current version out dir
CUR_VER_DIR=`date +%Y%m%d`
OUTPUT_DIR=${BUILD_HOME}/${CUR_VER_DIR}
rm -rf ${OUTPUT_DIR}
mkdir -p ${OUTPUT_DIR}

chmod ugo+x ${BUILD_DIR}/installer/linux/*.sh
#ant build CMT
java -jar ${ECLIPSE_HOME}/plugins/org.eclipse.equinox.launcher_1.1.0.v20100507.jar -application org.eclipse.ant.core.antRunner -buildfile ${BUILD_DIR}/buildProduct.xml -Doutput.path=${OUTPUT_DIR} -Declipse.home=${ECLIPSE_HOME} distlinux

#package the source file
VER_FILE=${BUILD_HOME}/trunk/com.cubrid.cubridmigration.ui/version.properties
VERSION=`awk -F"=" '/buildVersionId=.+/ {print $2}' ${VER_FILE}| sed 's/\r//g'`
GZ_SOURCE_FILE=${PRODUCT_NAME}-${VERSION}-source.tar.gz
java -jar ${ECLIPSE_HOME}/plugins/org.eclipse.equinox.launcher_1.1.0.v20100507.jar -application org.eclipse.ant.core.antRunner -buildfile ${BUILD_DIR}/buildProduct.xml -Doutput.path=${OUTPUT_DIR} -Declipse.home=${ECLIPSE_HOME} cleanTrash

cd ${BUILD_HOME}/trunk
tar -czf ${OUTPUT_DIR}/${GZ_SOURCE_FILE} *
cd ${BUILD_HOME}