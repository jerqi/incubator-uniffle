#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This script get am log of a appId, echo appId | python get-amlog.py
"""

import json
import sys
import urllib2

URL_BASE = "http://tdwbi.oa.com/cgi-bin/get_app_info.cgi?id=%s"
URL_TAIL = "/stderr/?start=0"

def httpGet(url):
  try:
    content = urllib2.urlopen(url).read()
  except Exception, e:
    print >> sys.stderr, e
    return None
  return content

def getContainerlogUrl(jsonStr):
  try:
    jsonObj = json.loads(jsonStr)
    url = jsonObj.get("info", None).get("amContainerLogs", None)
    return url + URL_TAIL
  except Exception, e:
    print >> sys.stderr, e
    return None

for line in sys.stdin:
  appId = line.strip()
  url = URL_BASE % appId
  jsonStr = httpGet(url)
  amUrl = getContainerlogUrl(jsonStr)
  with open("am_log/" + appId, "wb") as f:
    print >> f, httpGet(amUrl)

