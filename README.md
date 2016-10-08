Statement Cache Test - demo application

Simple demo application to see if statement caching on JDBC driver level has some performance gains.
A comparison is done with executing 25 of the same queries against a DB2 LUW 9.7 database.
25 Queries are executed once without statement caching enabled, and one time with statement caching enabled.
The results of the tests are outputted to the console and can be only total based or including the details of all queries executed.

This demo application is described on my blog site: https://blog.bertvanlangen.com/software-development/jdbc-statement-cache/
