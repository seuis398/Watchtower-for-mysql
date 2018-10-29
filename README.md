Watchtower (for MySQL)
======================

### Watchtower
Watchtower is a python application designed for monitoring multiple mysql instances. 

### Requirements
- Python 2.6 or higher
- Python/MySQLdb
```
$ yum install MySQL-python
```

### Usage
#### 1) Create server list file.
Server list file is a CSV file that contains group name, hostname, mysql port. See "example.txt".
```
$ cat example.txt
group1,test1,3306
group1,test2,3306
group2,test3,3307
group2,test4,3307
```
#### 2) Create a MySQL user account 
```
GRANT REPLICATION CLIENT ON *.*  to {USER}@{MONITOR_SERVER_IP} IDENTIFIED BY '{PASSWORD}';
```
#### 3) Run watchtower
```
$ python watchtower.py serverlist.txt
MySQL Username : ****
MySQL Password : ****

View Mode : 0.Basic, 1.InnoDB, 2.Replication, 3.Handler, 4.Sort&Temp, 5.Network
Interval  : 3 sec.
Command   : (X or Q)Exit, (F)FileLogging, (G)GroupSum, (W)WriteSummary, (R)ResetΣ, (+|-)Interval

---------------------------------------------------------------------------------------------------------------------------
 ServerName  Port  Conn Run Ab AbΣ  Select Update Insert Delete Replace   QPS Slow SlowΣ  RO   IO  SQL Delay  Version  GTID
---------------------------------------------------------------------------------------------------------------------------

## group1
 test1.mydb  3306   618  10  0   0    7810      2     24     10       0  7846    0     2      Yes  Yes     0   5.6.32   OFF
 test2.mydb  3306   613   7  0   0    7931      2     23     10       0  7966    0     0  ON  Yes  Yes     0   5.6.32   OFF

## group2
 test3.mydb  3306  3691  23  0   0    4612    496     80     34       5  5222    0     0      Yes  Yes     0   5.7.22    ON
 test4.mydb  3306  3402  11  0   0    4141    387    101     29       7  4658    0     0      Yes  Yes     0   5.7.22    ON
 test5.mydb  3306    11   2  0   0       1    883    181     62      12  1140    0     0  ON  Yes  Yes     1   5.7.22    ON

 >> Elapsed time : 0.021511
```

or, You can use hard-coded id/password. (line #14 ~ 16)
```
14 # Global Variable
15 MySQL_User="****"
16 MySQL_Pass="****"
```
