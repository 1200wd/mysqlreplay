Mysqlreplay
===========

Replays the MySQL General query log described here: http://dev.mysql.com/doc/refman/5.5/en/query-log.html

It opens a new connection for every connection id encountered in the query log, but it doesn't honour the timestamps.

Usage
-----

 $ ./mysqlreplay.php -fmysql.log -s127.0.0.1 -uusername -ppass
