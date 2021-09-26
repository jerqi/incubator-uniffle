# -*-coding: utf-8-*-

"""
usage: python get-metrics.py [fileName] [port] [metricsName]
"""

import json
import sys
import urllib2

URL_BASE = "http://%s:%s/metrics/server"

def usage():
  print "usage: python get-metrics.py [fileName] [port] [metricsName]"

def httpGet(url):
  try:
    content = urllib2.urlopen(url).read()
  except Exception, e:
    print >> sys.stderr, e
    return None
  return content

def parseMetrics(jsonStr):
  ret = dict()
  jsonObj = json.loads(jsonStr)
  metrics = jsonObj.get("metrics", None)
  if metrics is None:
    print >> sys.stderr, "metrics is invalid, jsonStr is %s" % jsonStr
    return ret
  for item in metrics:
    name = item.get("name", None)
    if name is None:
      print >> sys.stderr, "%s is invalid, jsonStr is %s" % (str(item), jsonStr)
      continue
    value = item.get("value", None)
    if value is None:
      print >> sys.stderr, "%s is invliad, jsonStr is %s" % (str(item), jsonStr)
      continue
    ret[name] = value
  return ret

def getAllMetrics(url):
  content = httpGet(url)
  if content is None:
    return dict()
  return parseMetrics(content)

def getMetrics(url, name):
  metrics = getAllMetrics(url)
  val = metrics.get(name, None)
  if val is None:
    print >> sys.stderr, "Fail to get %s using %s, metrics is %s" % (name, url, metrics)
  return val

def assembleUrl(line, port):
  try:
    ip, _ = line.strip().split("-")
    return URL_BASE % (ip, port)
  except Exception, e:
    print >> sys.stderr, "fail to assemble %s" % line
    print >> sys.stderr, e

def main():
  args = sys.argv
  if len(args) != 4:
    usage()
    sys.exit(1)
  with open(args[1], 'rb') as f:
    for line in f:
      url = assembleUrl(line, args[2])
      m = getMetrics(url, args[3])
      print "Node %s , Metrcis[%s] %s" % (line.strip(), args[3], m)

if __name__ == '__main__':
  main()
