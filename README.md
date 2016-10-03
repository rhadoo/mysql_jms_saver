# mysql_jms_saver
Tool to save jms messages from OpenMQ based enhanced broker clusters runing on top of MySQL/MariaDb.
This utility allows fast save/backup of messages on a broker cluster, by saving messages directly from underlying database.
Messages are saved in .zip archives, compatible with QBrowser, for fast inspection, redelivery.

Proper Usage is: java -jar mysqlsaver.jar ip dbname user pass table queuename(all if unspecified)