#!/usr/bin/python

"""
This script parse rss coordinator's log and get status of finished appId,
download am log of unsuccessful apps by calling get-amlog.py and report total
count of failed app to zhiyan.
nohup python app-status-checker.py /data/rssadmin/rssenv/rss/logs/rss.log appResult.txt -1 > app_status.log &
"""

import re
import sys
import time
import os
import commands
import platform
from datetime import datetime

REPORT_TOOL = "/usr/local/zhiyan/agent/bin/report_tool"
APP_MARK = "2540_11500_RSS"
INSTANCE_MARK = platform.node()
REPORT_COMMAND = "%s -app_mark %s -instance_mark %s -calc_method 0 " % (REPORT_TOOL, APP_MARK, INSTANCE_MARK)
DEFAULT_MAIN_NAME = "CoordinatorServer"
TAG_DICT = {"service_name": DEFAULT_MAIN_NAME}
DEFAULT_METRIC_NAME = "total_fail_app_num"
TOTAL_FAIL_APP_NUM_DIR = {DEFAULT_METRIC_NAME: 0}

def makeKVByDict(kv):
  kvList = list()
  for k, v in kv.iteritems():
    cur = k + "=" + str(v)
    kvList.append(cur)
  return "&".join(kvList)

def get_status(app_id):
  url = "http://100.76.25.135:8080/cluster/app/" + app_id
  result = commands.getoutput("curl " + url)
  prefix = "<th>.*\n.*FinalStatus Reported by AM:.*\n.*</th>.*\n.*<td>.*\n.*"
  match = re.search(prefix, result)
  if match is None:
    return "not found"
  else:
    match = re.search(prefix + "SUCCEEDED", result)
    if match is None:
      match = re.search(prefix + "FAILED", result)
      if match is None:
        match = re.search(prefix + "KILLED", result)
        if match is None:
          return "other"
        else:
          return "KILLED"
      else:
        return "FAILED"
    else:
      return "SUCCEEDED"


def process_line(line, output, is_process):
  if "Remove expired application" in line:
    match = re.search("application:.*", line)
    if match is not None:
      app_id_rss = line[match.start() + 12:match.end() - 1]
      app_id = app_id_rss[:app_id_rss.rindex("_")]
      if is_process:
        status = get_status(app_id)
        with open(output, 'a') as f:
          f.write(app_id + "," + status + "\n")
        if status != "not found" and status != "SUCCEEDED":
          TOTAL_FAIL_APP_NUM_DIR[DEFAULT_METRIC_NAME] += 1
          try:
            commands.getoutput("echo " + app_id + " | python get-amlog.py ")
          except:
            return ""
        return ""
      else:
        return app_id
  return ""


def get_size_with_retry(src_file):
  i = 0
  while True:
    try:
      return os.path.getsize(src_file)
    except:
      if i < 3:
        time.sleep(10)
        i = i + 1
      else:
        raise

def reportMetrics(mdict):
  tags = "-tag_set \"" + makeKVByDict(TAG_DICT) + "\" "
  metrics = "-metric_val \"" + makeKVByDict(mdict) + "\" "
  send_report_command = REPORT_COMMAND + tags + metrics
  #print send_report_command
  status, ret = commands.getstatusoutput(send_report_command)

  now = datetime.now()
  dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
  if status != 0:
    print send_report_command
    print status, ret, dt_string

if __name__ == '__main__':
  src = sys.argv[1]
  out = sys.argv[2]
  start_app_id = sys.argv[3]
  processed_bytes = 0

  start_work = False
  if start_app_id == "-1":
    start_work = True

  while True:
    src_size = get_size_with_retry(src)
    if src_size > processed_bytes:
      read_f = open(src, 'r')
      read_f.seek(processed_bytes)
      while True:
        line = read_f.readline()
        if not line:
          break
        current_app_id = process_line(line, out, start_work)
        if start_work is False and start_app_id == current_app_id:
          start_work = True
        processed_bytes = processed_bytes + len(line)
      read_f.close()
    elif src_size < processed_bytes:
      processed_bytes = 0
    reportMetrics(TOTAL_FAIL_APP_NUM_DIR)
    time.sleep(60)
