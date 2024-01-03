import hashlib
import json
import random
import sys
import time

import requests as req
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def tof4_sign(timestamp, token, nonce):
    x = hashlib.sha256()
    string = timestamp + token + nonce + timestamp
    x.update(string.encode())
    return x.hexdigest().upper()


class MyKey:
    def __init__(self):
        self.Paasid = ""
        self.Token = ""


class Tof4Mail:
    MAIL_TX = "1"
    MAIL_INTERNET = "0"
    HTML_FORMAT = "1"
    TEXT_FORMAT = "0"

    def __init__(self):
        self.EmailType = self.MAIL_TX
        self.From = ""
        self.To = ""
        self.CC = ""
        self.Bcc = ""
        self.Title = ""
        self.Content = ""
        self.BodyFormat = self.HTML_FORMAT
        self.Priority = "0"

    def funcSetHeader(self, Mykey):
        header = {}
        timestamp = str(int(time.time()))
        nonce = str(random.randint(1000, 9999))
        signature = tof4_sign(timestamp, Mykey.Token, nonce)
        header["x-rio-paasid"] = Mykey.Paasid
        header["x-rio-nonce"] = nonce
        header["x-rio-timestamp"] = timestamp
        header["x-rio-signature"] = signature
        return header

    def funcSetBody(self):
        body = {
            "EmailType": self.EmailType,
            "From": self.From,
            "To": self.To,
            "CC": self.CC,
            "Bcc": self.Bcc,
            "Title": self.Title,
            "Content": self.Content,
            "BodyFormat": self.BodyFormat,
        }
        return json.dumps(body)

    def Send(self, URL, Mykey, Max_retries):
        headers = self.funcSetHeader(Mykey)
        body = self.funcSetBody()
        req1 = req.Session()
        retries = Retry(total=Max_retries, backoff_factor=random.uniform(0, 0.1))
        req1.mount("http://", HTTPAdapter(max_retries=retries))
        req1.mount("https://", HTTPAdapter(max_retries=retries))
        try:
            response = req1.post(url=URL, headers=headers, data=body, timeout=5)
            print(response.content)
            print(response.status_code)
        except req.exceptions.RequestException as e:
            print(e)


if __name__ == "__main__":
    msg = Tof4Mail()
    msg.Title = sys.argv[1] + "性能测试报告"
    with open(sys.argv[2],'r') as f:
        s=''
        for l in f.readlines():
            s+='<p>'+l.replace('\n', '')+'</p>'
        msg.Content=s
    msg.From = sys.argv[6]
    if len(sys.argv) > 7:
        msg.To = sys.argv[7]
    else:
        msg.To = ''
    msg.CC = ''
    msg.Bcc = ''

    key = MyKey()
    key.Paasid = sys.argv[3]
    key.Token = sys.argv[4]

    max_retries = 3
    msg.Send(URL=sys.argv[5], Mykey=key, Max_retries=max_retries)
