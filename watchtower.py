#!/usr/bin/env python  
#coding=utf-8         
import sys
import os
import csv
import thread
import time
import getpass
import MySQLdb
import MySQLdb.cursors

WT_VERSION="0.6"

# Global Variables
MySQL_User=""
MySQL_Pass=""

ViewMode=0
RefreshInterval=3
CmdGroupSum=False
CmdFileLogging=False
CmdReset=False
CmdExit=False


#################################################################
class color:
  red = '\033[91m'
  green = '\033[92m'
  yellow = '\033[93m'
  blue = '\033[94m'
  magenta = '\033[95m'
  cyan = '\033[96m'
  bold = '\033[1m'
  underline = '\033[4m'
  bold_underline = '\033[1;4m'
  reset = '\033[0m'


#################################################################
class MyInstance:

  def __init__(self, groupname, hostname, port):
    self.groupname = groupname
    self.hostname = hostname 
    self.port = port
    self.connected = False
    self.prev_stat = {'dummy':0} #dict()
    self.curr_stat = {'dummy':0} # dict()
    self.sum_slow = 0
    self.sum_abo = 0

  def __del__(self):
    self.disconnect()

  def connect(self, user, passwd):
    try:
      self.dbconn = MySQLdb.connect(host=self.hostname, port=self.port, user=user, passwd=passwd, connect_timeout=1)
      self.dbcur = self.dbconn.cursor(MySQLdb.cursors.DictCursor)
      self.update_stat()
      self.connected = True
    except MySQLdb.Error as err:
      self.connected = False
      # print err (member 변수에 에러 내용 심어주자.)

  def disconnect(self):
    if self.connected == True :
      self.dbcur.close()
      self.dbconn.close()

  def update_stat(self):
    self.prev_stat.clear()
    self.prev_stat = self.curr_stat.copy()
    self.curr_stat.clear()

    try:
      # global status
      self.dbcur.execute("SHOW GLOBAL STATUS")
      for row in self.dbcur:
        self.curr_stat[row["Variable_name"].lower()] = row["Value"]

      # global variable (read_only, version, gtid_mode)
      self.dbcur.execute("SHOW GLOBAL VARIABLES LIKE 'read_only'")
      for row in self.dbcur:
        self.curr_stat[row["Variable_name"].lower()] = row["Value"]

      self.dbcur.execute("SHOW GLOBAL VARIABLES LIKE 'version'")
      for row in self.dbcur:
        self.curr_stat[row["Variable_name"].lower()] = row["Value"].split("-")[0]

      self.dbcur.execute("SHOW GLOBAL VARIABLES LIKE 'gtid_mode'")
      for row in self.dbcur:
        self.curr_stat[row["Variable_name"].lower()] = row["Value"]

      # slave status
      self.curr_stat["master_log_file"] = []
      self.curr_stat["read_master_log_pos"] = []
      self.curr_stat["relay_master_log_file"] = []
      self.curr_stat["exec_master_log_pos"] = []
      self.curr_stat["slave_io_running"] = []
      self.curr_stat["slave_sql_running"] = []
      self.curr_stat["last_error"] = []
      self.curr_stat["seconds_behind_master"] = []
      self.curr_stat["channel_name"] = []

      self.dbcur.execute("SHOW SLAVE STATUS")
      for row in self.dbcur:
        self.curr_stat["master_log_file"].append(row["Master_Log_File"])
        self.curr_stat["read_master_log_pos"].append(row["Read_Master_Log_Pos"])
        self.curr_stat["relay_master_log_file"].append(row["Relay_Master_Log_File"])
        self.curr_stat["exec_master_log_pos"].append(row["Exec_Master_Log_Pos"])
        self.curr_stat["slave_io_running"].append(row["Slave_IO_Running"])
        self.curr_stat["slave_sql_running"].append(row["Slave_SQL_Running"])

        if row["Seconds_Behind_Master"] is None:
          self.curr_stat["seconds_behind_master"].append(0)
        else:
          self.curr_stat["seconds_behind_master"].append(int(row["Seconds_Behind_Master"]))

        self.curr_stat["last_error"].append(row["Last_Error"])

        # mysql 5.7 ~
        try:
          self.curr_stat["channel_name"].append(row["Channel_Name"])
        except KeyError:
          self.curr_stat["channel_name"].append("")

      return True

    except MySQLdb.Error as err:
      self.connected = False
      return False

  def get_current(self, item):
    if self.curr_stat.get(item) == None:
      return ""
    else:
      return self.curr_stat.get(item)

  def get_delta(self, item):
    if self.curr_stat.get(item) == None:
      return 0
    else:
      return int(self.curr_stat.get(item)) - int(self.prev_stat.get(item))

  def get_per_sec(self, item):
    if self.curr_stat.get(item) == None:
      return 0
    else:
      delta = int(self.curr_stat.get(item)) - int(self.prev_stat.get(item))
      sec   = int(self.curr_stat.get('uptime')) - int(self.prev_stat.get('uptime'))

      if delta < 0:
        delta = 0
   
      return int(delta / sec)
 
  def get_repl_channel_cnt(self):
    return max(1, len(self.curr_stat["master_log_file"]))
 
  def get_repl_summary(self):
    ret = []
    io_err_cnt = 0
    sql_err_cnt = 0
    max_delay = 0

    try:
      if len(self.curr_stat["master_log_file"]) == 0:
        ret = ["-", "-", 0]
      else:
        for idx in range(0, len(self.curr_stat["master_log_file"])):
          if self.curr_stat["slave_io_running"][idx] != "Yes":
            io_err_cnt = io_err_cnt + 1

          if self.curr_stat["slave_sql_running"][idx] != "Yes":
            sql_err_cnt = sql_err_cnt + 1

          if max_delay < self.curr_stat["seconds_behind_master"][idx]: 
            max_delay = self.curr_stat["seconds_behind_master"][idx]
  
        if io_err_cnt == 0:
          ret.append("Yes")
        else:
          ret.append("No/" + str(io_err_cnt))

        if sql_err_cnt == 0:
          ret.append("Yes")
        else:
          ret.append("No/" + str(sql_err_cnt))

        ret.append(max_delay)
    except:
      ret = ["-", "-", 0]

    return ret

  def get_repl_detail(self):
    ret = []

    if len(self.curr_stat["master_log_file"]) == 0:
      ret.append(["", "", "", "", "-", "-", "", 0, ""])
    else:
      for idx in range(0, len(self.curr_stat["master_log_file"])):
        io_thread = "Yes" if self.curr_stat["slave_io_running"][idx] == "Yes" else "No"
        sql_thread = "Yes" if self.curr_stat["slave_sql_running"][idx] == "Yes" else "No"

        ret.append( [ self.curr_stat["master_log_file"][idx], self.curr_stat["read_master_log_pos"][idx], \
                    self.curr_stat["relay_master_log_file"][idx], self.curr_stat["exec_master_log_pos"][idx], \
                    io_thread, sql_thread, self.curr_stat["last_error"][idx], 
                    self.curr_stat["seconds_behind_master"][idx], self.curr_stat["channel_name"][idx] ] )

    return ret


def read_single_keypress():
    import termios, fcntl, sys, os
    fd = sys.stdin.fileno()

    # save old state
    flags_save = fcntl.fcntl(fd, fcntl.F_GETFL)
    attrs_save = termios.tcgetattr(fd)

    # make raw - the way to do this comes from the termios(3) man page.
    attrs = list(attrs_save) # copy the stored version to update
    # iflag
    attrs[0] &= ~(termios.IGNBRK | termios.BRKINT | termios.PARMRK | termios.ISTRIP | termios.INLCR | termios.IGNCR | termios.ICRNL | termios.IXON)
    # oflag
    # attrs[1] &= ~termios.OPOST
    # cflag
    # attrs[2] &= ~(termios.CSIZE | termios.PARENB)
    # attrs[2] |= termios.CS8
    # lflag
    attrs[3] &= ~(termios.ECHONL | termios.ECHO | termios.ICANON | termios.ISIG | termios.IEXTEN)

    termios.tcsetattr(fd, termios.TCSANOW, attrs)

    # turn off non-blocking
    fcntl.fcntl(fd, fcntl.F_SETFL, flags_save & ~os.O_NONBLOCK)

    # read a single keystroke
    try:
        ret = sys.stdin.read(1) # returns a single character
    except KeyboardInterrupt: 
        ret = 0
    finally:
        # restore old state
        termios.tcsetattr(fd, termios.TCSAFLUSH, attrs_save)
        fcntl.fcntl(fd, fcntl.F_SETFL, flags_save)

    return ret


def input_thread():
   global ViewMode
   global RefreshInterval
   global CmdExit
   global CmdFileLogging
   global CmdGroupSum
   global CmdReset

   while True:
      ch = read_single_keypress().lower()
 
      if ch == "0" or ch == "1" or ch == "2" or ch == "3" or ch == "4" or ch == "5":
         ViewMode = int(ch)
      elif ch == "x" or ch == 'q' :
         CmdExit = True
	 break
      elif ch == "f":
         CmdFileLogging = toggle(CmdFileLogging)
      elif ch == 'g':
         CmdGroupSum = toggle(CmdGroupSum)
      elif ch == "r":
         CmdReset = True
      elif ch == "+":
        if RefreshInterval < 30: 
          RefreshInterval = RefreshInterval + 1
      elif ch == "-":
        if RefreshInterval > 1:
          RefreshInterval = RefreshInterval - 1


def make_line(ch, n):
  ln = ""
  while n > 0:
    ln = ln + ch
    n = n-1

  print ln


def print_header():
  os.system("clear")

  print ""
  print "View Mode : %s, %s, %s, %s, %s, %s" % \
       ( (color.bold_underline + "0.Basic" + color.reset) if ViewMode == 0 else "0.Basic",
         (color.bold_underline + "1.InnoDB" + color.reset) if ViewMode == 1 else "1.InnoDB",
         (color.bold_underline + "2.Replication" + color.reset) if ViewMode == 2 else "2.Replication",
         (color.bold_underline + "3.Handler" + color.reset) if ViewMode == 3 else "3.Handler",
         (color.bold_underline + "4.Sort&Temp" + color.reset) if ViewMode == 4 else "4.Sort&Temp",
         (color.bold_underline + "5.Network" + color.reset) if ViewMode == 5 else "5.Network")
  print "Interval  : %d sec." % RefreshInterval
  print "Command   : %s, %s, %s, %s, %s" % \
       ( "(X or Q)Exit", # (color.bold_underline + "(X or Q)Exit" + color.reset) if CmdExit == True else "(X or Q)Exit",
         (color.bold_underline + "(F)FileLogging" + color.reset) if CmdFileLogging == True else "(F)FileLogging", 
         (color.bold_underline + "(G)GroupSum" + color.reset) if CmdGroupSum == True else "(G)GroupSum",
         "(R)ResetΣ", #(color.bold_underline + "(R)ResetΣ" + color.reset) if CmdReset == True else "(R)ResetΣ",
         "(+|-)Interval") #(color.bold_underline + "(+/-)Interval" + color.reset) if CmdExit == True else "(+|-)Interval")
  print "" 

  if ViewMode == 0:
    make_line("-", 128)
    print "%16s %5s %5s %3s %2s %3s  %6s %6s %6s %6s %7s %5s %4s %5s  %2s %4s %4s %5s  %7s  %4s" % \
         ("ServerName", "Port", "Conn", "Run", "Ab", "AbΣ", "Select", "Update", "Insert", "Delete", "Replace", "QPS",
          "Slow", "SlowΣ", "RO", "IO", "SQL" , "Delay", "Version", "GTID")
    make_line("-", 128)

  elif ViewMode == 1:
    make_line("-", 162)
    print "%16s %5s %5s %3s %2s %3s  %6s %6s %6s %6s %7s %5s %4s %5s  %2s %4s %4s %5s  %7s %7s %7s %8s %6s %7s" % \
         ("ServerName", "Port", "Conn", "Run", "Ab", "AbΣ", "Select", "Update", "Insert", "Delete", "Replace", "QPS",  
          "Slow", "SlowΣ", "RO", "IO", "SQL" , "Delay", "R.Read", "R.Write", "Logical", "Physical", "B.Hit%", "B.Dirty")
    make_line("-", 162)

  elif ViewMode == 2:
    make_line("-", 184)  
    print "%16s %5s %5s %3s %2s %3s  %6s %6s %6s %6s %7s %5s %4s %5s  %2s  %7s %3s %3s  %16s %10s  %16s %10s %5s %5s" % \
         ("ServerName", "Port", "Conn", "Run", "Ab", "AbΣ", "Select", "Update", "Insert", "Delete", "Replace", "QPS",
          "Slow", "SlowΣ", "RO", "Channel", "IO", "SQL", "Master_Log", "Master_Pos", "Relay_M_Log", "Exec_Pos", "Delay", "Error")
    make_line("-", 184)

  elif ViewMode == 3:
    make_line("-", 170)
    print "%16s %5s %5s %3s %2s %3s  %6s %6s %6s %6s %7s %5s %4s %5s  %2s %4s %4s %5s  %7s %7s %7s %7s %7s %7s %7s" % \
         ("ServerName", "Port", "Conn", "Run", "Ab", "AbΣ", "Select", "Update", "Insert", "Delete", "Replace", "QPS",
          "Slow", "SlowΣ", "RO", "IO", "SQL" , "Delay", "Key", "Next", "Prev", "Rnd", "RndNext", "Update", "Write")
    make_line("-", 170)

  elif ViewMode == 4:
    make_line("-", 143)
    print "%16s %5s %5s %3s %2s %3s  %6s %6s %6s %6s %7s %5s %4s %5s  %2s %4s %4s %5s  %8s %10s %8s" % \
         ("ServerName", "Port", "Conn", "Run", "Ab", "AbΣ", "Select", "Update", "Insert", "Delete", "Replace", "QPS",
          "Slow", "SlowΣ", "RO", "IO", "SQL" , "Delay", "SortRows", "MemoryTemp", "DiskTemp")
    make_line("-", 143) 

  elif ViewMode == 5:
    make_line("-", 130)
    print "%16s %5s %5s %3s %2s %3s  %6s %6s %6s %6s %7s %5s %4s %5s  %2s %4s %4s %5s  %7s %7s" % \
         ("ServerName", "Port", "Conn", "Run", "Ab", "AbΣ", "Select", "Update", "Insert", "Delete", "Replace", "QPS",
          "Slow", "SlowΣ", "RO", "IO", "SQL" , "Delay", "NetRecv", "NetSent")
    make_line("-", 130)


def toggle(v):
  if v == True:
    return False
  else:
    return True

 
#
# MAIN
#
if __name__ == '__main__':
  mys = []
  fileheader = 0

  # read server list file
  if len(sys.argv) < 2:
    print "Usage : python %s serverlist.txt" % sys.argv[0]
    exit(1)

  # print watchtower version
  if sys.argv[1].lower() == "version":
    print "watchtower %s" % WT_VERSION
    exit(1)

  # read mysql user/passwd
  if len(MySQL_User) == 0:
    MySQL_User = raw_input("MySQL Username : ")

  if len(MySQL_Pass) == 0:
    MySQL_Pass = getpass.getpass("MySQL Password : ")

  try:
    with open(sys.argv[1], "r") as fd:
      reader = csv.reader(fd)
      for row in reader:
        if len(row) == 3:
          mi = MyInstance(row[0].strip(), row[1].strip(), int(row[2].strip()) )
          mi.connect(MySQL_User, MySQL_Pass) # ??? 
          mys.append(mi)
    fd.close()
  except IOError:
    print "ERROR: Can't open file: ", sys.argv[1]
    exit(1)
  #finally:

  # key stroke checker
  thread.start_new_thread(input_thread, () )  

  # View
  while True :
    prev_group = ""
    time.sleep(RefreshInterval)
    ts_start = time.time()
   
    print_header()

    for mi in mys: 
      if prev_group != "" and prev_group != mi.groupname and CmdGroupSum == True:
        print "%33s %3d %2d %3d  %6d %6d %6d %6d %7d %5d %4d %5d %s" % \
             (color.red, acc_run, acc_ab, acc_sum_abo, acc_select, acc_update, acc_insert,
              acc_delete, acc_replace, acc_qps, acc_slow, acc_sum_slow, color.reset)

      if prev_group != mi.groupname:
        print "\n##", mi.groupname
        acc_run = 0
        acc_ab = 0
        acc_sum_abo = 0
        acc_select = 0
        acc_update = 0
        acc_insert = 0
        acc_delete = 0
        acc_replace = 0
        acc_qps = 0
        acc_slow = 0
        acc_sum_slow = 0        
 
      if mi.connected == True: 
        mi.update_stat( )
   
        run = int(mi.get_current('threads_running')) if mi.get_current('threads_running') != '' else 0
        abo = mi.get_delta('aborted_connects')
        com_sel = mi.get_per_sec('com_select') + mi.get_per_sec('qcache_hits')
        com_upd = mi.get_per_sec('com_update') + mi.get_per_sec('com_update_multi')  
        com_del = mi.get_per_sec('com_delete') + mi.get_per_sec('com_delete_multi')
        com_ins = mi.get_per_sec('com_insert') + mi.get_per_sec('com_insert_select')
        com_rep = mi.get_per_sec('com_replace')
        com_qps = com_sel + com_upd + com_del + com_ins + com_rep
        slow = mi.get_delta('slow_queries')
        inno_row_read = mi.get_per_sec('innodb_rows_read')
        inno_row_write = mi.get_per_sec('innodb_rows_inserted') + mi.get_per_sec('innodb_rows_updated') + mi.get_per_sec('innodb_rows_deleted')
  
        # ResetΣ"
        if CmdReset == True:
          mi.sum_slow = slow
          mi.sum_abo = abo
        else:
          mi.sum_slow = mi.sum_slow + slow
          mi.sum_abo = mi.sum_abo + abo

        # for GroupSum
        if CmdGroupSum == True:
          acc_run = acc_run + run
          acc_ab = acc_ab + abo
          acc_sum_abo = acc_sum_abo + mi.sum_abo
          acc_select = acc_select + com_sel
          acc_update = acc_update + com_upd
          acc_insert = acc_insert + com_ins
          acc_delete = acc_delete + com_del
          acc_replace = acc_replace + com_rep
          acc_qps = acc_qps + com_qps
          acc_slow = acc_slow + slow 
          acc_sum_slow = acc_sum_slow + mi.sum_slow
        
        repl_summary = [] 
        repl_summary = mi.get_repl_summary()

        memory_tmp = mi.get_per_sec('created_tmp_tables') - mi.get_per_sec('created_tmp_disk_tables')
        disk_tmp = mi.get_per_sec('created_tmp_disk_tables')

        if ViewMode == 0 and mi.connected == True:  #Basic 
          print "%16s %5d %5s %3d %2d %3d  %6d %6d %6d %6d %7d %5d %4d %5d  %2s %4s %4s %5d  %7s  %4s" % \
               (mi.hostname, mi.port, mi.get_current('threads_connected'), run,
                abo, mi.sum_abo, com_sel, com_upd, com_ins, com_del, com_rep, com_qps, slow, mi.sum_slow,
                mi.get_current('read_only').replace('OFF', ''), repl_summary[0], repl_summary[1], repl_summary[2],
                mi.get_current('version'), mi.get_current('gtid_mode')
               )

        elif ViewMode == 1 and mi.connected == True:  #InnoDB
          print "%16s %5d %5s %3d %2s %3d  %6d %6d %6d %6d %7d %5d %4d %5d  %2s %4s %4s %5d  %7d %7d %7d %8d %5.1f%% %5dmb" % \
               (mi.hostname, mi.port, mi.get_current('threads_connected'), run,
                abo, mi.sum_abo, com_sel, com_upd, com_ins, com_del, com_rep, com_qps, slow, mi.sum_slow,
                mi.get_current('read_only').replace('OFF', ''), repl_summary[0], repl_summary[1], repl_summary[2],
                inno_row_read, inno_row_write, mi.get_per_sec('innodb_buffer_pool_read_requests'), mi.get_per_sec('innodb_buffer_pool_reads'),
                100.0 - (100.0 * mi.get_per_sec('innodb_buffer_pool_reads') / mi.get_per_sec('innodb_buffer_pool_read_requests')) if mi.get_per_sec('innodb_buffer_pool_read_requests') > 0 else 100.0,
                mi.get_current('innodb_buffer_pool_bytes_dirty') == '' and -1 or int(mi.get_current('innodb_buffer_pool_bytes_dirty')) / 1024 / 1024
              )

        elif ViewMode == 2 and mi.connected == True:  #Replication
          repl_detail = []
          repl_detail = mi.get_repl_detail()
          
          for idx in range(0, mi.get_repl_channel_cnt()):
            if idx == 0:
              print "%16s %5d %5s %3d %2d %3d  %6d %6d %6d %6d %7d %5d %4d %5d  %2s  %7s %3s %3s  %16s %10s  %16s %10s %5d %s" % \
                   (mi.hostname, mi.port, mi.get_current('threads_connected'), run,
                    abo, mi.sum_abo, com_sel, com_upd, com_ins, com_del, com_rep, com_qps, slow, mi.sum_slow,
                    mi.get_current('read_only').replace('OFF', ''),
                    repl_detail[idx][8], repl_detail[idx][4], repl_detail[idx][5],
                    repl_detail[idx][0], repl_detail[idx][1], repl_detail[idx][2], repl_detail[idx][3],
                    repl_detail[idx][7], repl_detail[idx][6]
                 )
            else: # multi-source replication
              print "%106s %3s %3s  %16s %10s  %16s %10s %5d %s" % \
                   (repl_detail[idx][8], repl_detail[idx][4], repl_detail[idx][5],
                    repl_detail[idx][0], repl_detail[idx][1], repl_detail[idx][2], repl_detail[idx][3],
                    repl_detail[idx][7], repl_detail[idx][6]
                   )

          del repl_detail

        elif ViewMode == 3 and mi.connected == True:  #Handler
          print "%16s %5d %5s %3d %2d %3d  %6d %6d %6d %6d %7d %5d %4d %5d  %2s %4s %4s %5d  %7d %7d %7d %7d %7d %7d %7d" % \
               (mi.hostname, mi.port, mi.get_current('threads_connected'), run,
                abo, mi.sum_abo, com_sel, com_upd, com_ins, com_del, com_rep, com_qps, slow, mi.sum_slow,
                mi.get_current('read_only').replace('OFF', ''), repl_summary[0], repl_summary[1], repl_summary[2],
                mi.get_per_sec('handler_read_key'), mi.get_per_sec('handler_read_next'), mi.get_per_sec('handler_read_prev'),
                mi.get_per_sec('handler_read_rnd'), mi.get_per_sec('handler_read_rnd_next'), mi.get_per_sec('handler_update'), mi.get_per_sec('handler_write') 
               )

        elif ViewMode == 4 and mi.connected == True:  #Sort&Temp
          print "%16s %5d %5s %3d %2d %3d  %6d %6d %6d %6d %7d %5d %4d %5d  %2s %4s %4s %5d  %8d %10d %8d" % \
               (mi.hostname, mi.port, mi.get_current('threads_connected'), run,
                abo, mi.sum_abo, com_sel, com_upd, com_ins, com_del, com_rep, com_qps, slow, mi.sum_slow,
                mi.get_current('read_only').replace('OFF', ''), repl_summary[0], repl_summary[1], repl_summary[2],
                mi.get_per_sec('sort_rows'), memory_tmp, disk_tmp
               )

        elif ViewMode == 5 and mi.connected == True:  #Net&FileIO
          print "%16s %5d %5s %3d %2d %3d  %6d %6d %6d %6d %7d %5d %4d %5d  %2s %4s %4s %5d  %5dkb %5dkb" % \
               (mi.hostname, mi.port, mi.get_current('threads_connected'), run,
                abo, mi.sum_abo, com_sel, com_upd, com_ins, com_del, com_rep, com_qps, slow, mi.sum_slow,
                mi.get_current('read_only').replace('OFF', ''), repl_summary[0], repl_summary[1], repl_summary[2],
                mi.get_per_sec('bytes_received') / 1024, mi.get_per_sec('bytes_sent') / 1024
               )

        if CmdFileLogging == True:
          try:
            filename = "" + mi.hostname + "_" + str(mi.port) + "_" + time.strftime('%Y%m%d') + ".log"
            tm = time.strftime('%H:%M:%S')

            with open(filename, "a") as fd:
              if fileheader == 0:
	        fd.write("===========================================================================================================================================================================================\n")
                fd.write("======== %-12s %-29s %-4s %-18s %-13s %-21s %-48s %-4s %-11s %-9s\n" % \
                         ("Connection", "Query process", "Slow", "Replication", "InnoDB(row)", "InnoDB(buffer)", "Handler", "Sort", "TempTable", "Network")
                        )
                fd.write("======== %-5s %-3s %-2s %-5s %-5s %-5s %-5s %-5s %-4s %-2s %-4s %-4s %-5s %-6s %-6s %-6s %-6s %-7s %-6s %-6s %-6s %-6s %-6s %-6s %-6s %-4s %-6s %-4s %-4s %-4s\n" % \
                         ("Conn", "Run", "Ab", "Sel", "Upd", "Ins", "Del", "Rep", "Slow", "RO", "IO", "SQL", "Delay", "Read", "Write", "L.Read", "P.Read", "Dirty",
                          "Key", "Next", "Prev", "Rnd", "RndNxt", "Update", "Write", "Rows", "Memory", "Disk", "Recv", "Sent")
                        )
		fd.write("===========================================================================================================================================================================================\n")

              fd.write("%8s %-5s %-3d %-2d %-5d %-5d %-5d %-5d %-5d %-4d %-2s %-4s %-4s %-5d %-6d %-6d %-6d %-6d %-7s %-6d %-6d %-6d %-6d %-6d %-6d %-6d %-4d %-6d %-4d %2dmb %2dmb\n" % \
                      (tm, mi.get_current('threads_connected'), run, abo, com_sel, com_upd, com_ins, com_del, com_rep, slow, 
                       mi.get_current('read_only').replace('OFF', ''), 
                       repl_summary[0], repl_summary[1], repl_summary[2],
                       inno_row_read, inno_row_write, mi.get_per_sec('innodb_buffer_pool_read_requests'), mi.get_per_sec('innodb_buffer_pool_reads'), 
                       mi.get_current('innodb_buffer_pool_bytes_dirty') == '' and "-1mb" or str(int(mi.get_current('innodb_buffer_pool_bytes_dirty')) / 1024 / 1024)+"mb",
                       mi.get_per_sec('handler_read_key'), mi.get_per_sec('handler_read_next'),
                       mi.get_per_sec('handler_read_prev'), mi.get_per_sec('handler_read_rnd'),
                       mi.get_per_sec('handler_read_rnd_next'), mi.get_per_sec('handler_update'),
                       mi.get_per_sec('handler_write'), mi.get_per_sec('sort_rows'), memory_tmp, disk_tmp,
                       mi.get_per_sec('bytes_received') / 1024 / 1024, mi.get_per_sec('bytes_sent') / 1024 / 1024)
                      )
          except IOError:
            print "ERROR: Can't open file: ", filename
          finally:
            fd.close()

        del repl_summary

      # reconnect
      if mi.connected == False:
        print "%16s %5d --x-- not connected or no privileges" % (mi.hostname, mi.port)
        mi.connect(MySQL_User, MySQL_Pass)                                                                                     

      prev_group = mi.groupname
    # end for

    if CmdFileLogging == True:
      fileheader = 0 if fileheader == 29 else fileheader + 1    
    else:
      fileheader = 0 

    if CmdGroupSum == True:
      print "%33s %3d %2d %3d  %6d %6d %6d %6d %7d %5d %4d %5d %s" % \
           (color.red, acc_run, acc_ab, acc_sum_abo, acc_select, acc_update, acc_insert,
            acc_delete, acc_replace, acc_qps, acc_slow, acc_sum_slow, color.reset)  

    CmdReset = False

    # print elapsed time
    ts_done = time.time()
    print "\n >> Elapsed time : %f\n" % (ts_done - ts_start)

    # flush stdout
    sys.stdout.flush()

    if (CmdExit == True):
      break
  # end while

  # Destroy 
  del mys
