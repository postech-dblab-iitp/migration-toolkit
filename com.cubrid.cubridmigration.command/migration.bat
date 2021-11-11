@echo off
SET fn=%1 %2 %3 %4 %5 %6 %7 %8 %9
java -Xms40M -Xmx1024M -jar migration.jar %fn%
@echo on