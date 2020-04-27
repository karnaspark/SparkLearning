# u'165.137.7.243 - 16621 [10/Feb/2014:23:59:52 +0100] "GET /KBDOC-00203.html HTTP/1.0" 200 14892 "http://www.loudacre.com"  "Loudacre Mobile Browser
#  Titanic 2000"

rd2 = sc.textFile("file:///home/hadoop/weblogs")

import re

def search_pattern(s):
    ip_pattern = re.compile(r"\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}") #Define pattern for IP Address
    ip = ip_pattern.search(s) #Extract IP Address from pattern
    return ip.group(0)

rd2.map(lambda rec: search_pattern(rec)).take(5)
