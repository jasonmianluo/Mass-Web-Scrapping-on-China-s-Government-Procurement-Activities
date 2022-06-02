import time
from threading import Timer
import requests
from time import sleep


class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer     = None
        self.function   = function
        self.interval   = interval
        self.args       = args
        self.kwargs     = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


def get_proxies(proxy_api):
    global proxy_IPs
    while True:
        try:
            ip_list = requests.get(proxy_api).text.split()
        except requests.RequestException as e:
            print('Retry getting proxies. {0}'.format(e))
            continue
        else:
            break
    proxies_list = []
    for i in range(0, len(ip_list)):
        proxies = {
            "http": "http://" + ip_list[i],
            "https": "https://" + ip_list[i]
        }
        proxies_list.append(proxies)
    print('{0} IPs obtained'.format(len(proxies_list)))
    # print(proxies_list[0])
    proxy_IPs = proxies_list


def test_new_proxies():
    global proxy_IPs
    time.sleep(2)
    print(proxy_IPs[0])


if __name__ == "__main__":
    proxy_ip_api = 'http://http.tiqu.letecs.com/getip3?num=3&type=1&pro=&city=0&yys=0' \
                   '&port=11&pack=235753&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions=&gm=4'
    proxy_IPs = []
    get_proxies(proxy_ip_api)

    rt = RepeatedTimer(10, get_proxies, proxy_ip_api)  # it auto-starts, no need of rt.start()
    try:
        while True:
            test_new_proxies()
            sleep(2)
    finally:
        print('should not be getting here')
