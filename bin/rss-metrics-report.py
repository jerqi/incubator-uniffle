# -*-coding: utf-8-*-

"""
This script collect metrics of shuffle server and coordinator and send to zhiyan
using report agent of zhiyan and the following is an example of zhiyan's report
agent tool,
/usr/local/zhiyan/agent/bin/report_tool -app_mark 35_580_app_mark -calc_method 0
  -instance_mark 192.168.1.1 -tag_set "ip=192.168.1.1&interface=upload"
  -metric_val "succ_cnt=1&total_cnt=100"
It also check the process existence and relaunch the shuffle server or coordinator.

Usage: python rss-metrics-report.py [MAIN_CLASSNAME] [METRICS_ENDPOINT] [WORK_DIR]
You could add a cron item in the server where shuffle server or coordinator deployed,
*/1 * * * * source ~/.bashrc && python /data/rssadmin/rssenv/metrics/rss-metrics-report.py >> rss.log 2>&1
*/1 * * * * source ~/.bashrc && python /data/rssadmin/rssenv/metrics/rss-metrics-report.py CoordinatorServer >> rss.log 2>&1

APP_MARK's format is $PROJECT_ID_$APPLICATION_ID_$APPLICATION_NAME, REPORT_TOOL is
path of the zhiyan report agent in the server where rss deployed.
"""


import commands
import json
import os
import time
import platform
import sys
import urllib2
import time
from datetime import datetime

REPORT_TOOL = "/usr/local/zhiyan/agent/bin/report_tool"
APP_MARK = "2540_11500_RSS"
INSTANCE_MARK = platform.node()
REPORT_COMMAND = "%s -app_mark %s -instance_mark %s -calc_method 0 " % (REPORT_TOOL, APP_MARK, INSTANCE_MARK)

SERVER_PATH = "/metrics/server"
JVM_PATH = "/metrics/jvm"

DEFAULT_MAIN_NAME = "ShuffleServer"
DEFAULT_METRICS_BASE_URL = "http://0.0.0.0:19998"
DEFAULT_WORK_DIR = "/data/rssadmin/rssenv/rss"

RESTART_SCRIPT_DICT = {
  "ShuffleServer": "bash ./bin/start-shuffle-server.sh",
  "CoordinatorServer": "bash ./bin/start-coordinator.sh"
}

# get java process' id and name
def getProcessIdName(process_name):
  ret, pidName = commands.getstatusoutput('jps | grep %s' % process_name)
  if ret != 0:
    return None, None
  else:
    pid, name = pidName.strip().split(" ")
    return int(pid), name

def restart(name, wdir):
  restartCommand = "cd %s;%s" % (wdir, RESTART_SCRIPT_DICT.get(name, "pwd"))
  print restartCommand
  os.system(restartCommand)

def makeKV(**kwargs):
  return "&".join(map(lambda x: x[0] + "=" + str(x[1]), kwargs.iteritems()))

def makeKVByDict(kv):
  kvList = list()
  for k, v in kv.iteritems():
    cur = k + "=" + str(v)
    kvList.append(cur)
  return "&".join(kvList)

def httpGet(url):
  try:
    content = urllib2.urlopen(url).read()
  except Exception, e:
    print e
    return None
  return content

def parseMetrics(jsonStr):
  ret = dict()
  jsonObj = json.loads(jsonStr)
  metrics = jsonObj.get("metrics", None)
  if metrics is None:
    print "metrics is invalid, jsonStr is %s" % jsonStr
    return ret
  for item in metrics:
    name = item.get("name", None)
    if name is None:
      print "%s is invalid, jsonStr is %s" % (str(item), jsonStr)
      continue
    value = item.get("value", None)
    if value is None:
      print "%s is invalid, jsonStr is %s" % (str(item), jsonStr)
      continue
    ret[name] = value
  return ret

def getMetrics(url):
  content = httpGet(url)
  if content is None:
    return dict()
  return parseMetrics(content)


def getTcp(pid):
  status, ret = commands.getstatusoutput(
      "netstat -napt | grep %d/java | grep ESTABLISHED | wc -l" % pid)
  if status != 0:
    return int(ret)
  else:
    return 0

def getCpuMem(pid):
  status, ret = commands.getstatusoutput("ps -p %d -o %%cpu,%%mem" % pid)
  ret = ret.strip().split("\n")
  if len(ret) != 2:
    return 0, 0
  ret = ret[1].strip().split(" ", 1)
  if len(ret) != 2:
    return 0, 0
  cpu = ret[0].strip()
  mem = ret[1].strip()
  return (cpu, mem)

def getProcessMetrics(pid):
  ret = dict()
  tcp = getTcp(pid)
  cpu, mem = getCpuMem(pid)
  ret["cpu"] = cpu
  ret["mem"] = mem
  ret["tcp"] = tcp
  return ret

def reportMetrics(mdict, tdict):
  tags = "-tag_set \"" + makeKVByDict(tdict) + "\" "
  metrics = "-metric_val \"" + makeKVByDict(mdict) + "\" "
  send_report_command = REPORT_COMMAND + tags + metrics
  status, ret = commands.getstatusoutput(send_report_command)
  now = datetime.now()
  dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
  if status != 0:
    print send_report_command
    print status, ret, dt_string

def main():
  args = sys.argv
  if len(args) >= 2:
    main_name = args[1]
  else:
    main_name = DEFAULT_MAIN_NAME
  if len(args) >= 3:
    metrics_base_url = args[2]
  else:
    metrics_base_url = DEFAULT_METRICS_BASE_URL
  if len(args) >= 4:
    work_dir = args[3]
  else:
    work_dir = DEFAULT_WORK_DIR
  #print args

  pid, name = getProcessIdName(main_name)

  retryCnt = 0
  while pid is None or name is None:
    if retryCnt >= 3:
      print "exceed retry count 3"
      sys.exit(1)
    restart(main_name, work_dir)
    print "sleep 1 after restart"
    time.sleep(1)
    pid, name = getProcessIdName(main_name)
    retryCnt = retryCnt + 1
  server_metrics_url = metrics_base_url + SERVER_PATH
  jvm_metrics_url = metrics_base_url + JVM_PATH
  m1 = getMetrics(server_metrics_url)
  m2 = getMetrics(jvm_metrics_url)
  m3 = getProcessMetrics(pid)
  m4 = {main_name : 1}
  mdict = dict()
  mdict.update(m1)
  mdict.update(m2)
  mdict.update(m3)
  mdict.update(m4)
  tdict = dict()
  tdict["service_name"] = main_name
  reportMetrics(mdict, tdict)

if __name__ == '__main__':
  main()