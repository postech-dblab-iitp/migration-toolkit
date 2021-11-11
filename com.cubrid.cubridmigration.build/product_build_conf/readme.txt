#The file layouts are supported by the dropins folder
#
#(1)The dropins folder supports a variety of layouts, depending on the scale of your application and the desired degree of separation of its parts. The simplest layout is to just drop plug-ins in either jar or directory format directly into the dropins folder:
#
# cubridmanager/
#   dropins/
#     com.cubrid.cubridmanager.nlucene.ui_8.4.0.201107271733.jar
#     com.cubrid.cubridmanager.nlucene.help_8.4.0.201107271733.jar
#     com.cubrid.cubridmanager.nlucene.ui_8.4.0.201107271733/
#       plugin.xml
#       tools.jar
#       ... etc ...
#   ...
#
#(2)You can also drop in the application or extension layout directly in the dropins folder:
#
# cubridmanager/
#   dropins/
#     eclipse/
#       features/
#      plugins/
#
# cubridmanager/
#   dropins/
#     queryeditor/
#       features/
#       plugins/
#       
#(3)If you have various different components being dropped in, and you want to keep them separate, you can add an additional layer of folders immediately below the dropins folder:
#
# cubridmanager/
#   dropins/
#     nlucene/
#       eclipse/
#         features/
#         plugins/
#     queryeditor/
#       eclipse/
#         features/
#         plugins/
#     ... etc ...
#
#
#(4)Finally, you can add link files as in the cubridmanager links folder:
#
# cubridmanager/
#   dropins/
#     nlucene.link
#
# nlucene.link file format:
# path=D:/plugin/nlucene
# 
# plugin directory format:
#      nlucene/
#       eclipse/
#         features/
#         plugins/