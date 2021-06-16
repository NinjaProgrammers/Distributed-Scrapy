import FlatServer
import chordServer
import logging
import random
from http.server import BaseHTTPRequestHandler, HTTPServer
import socket
import pickle
from urllib.parse import urlparse, unquote
import urllib.request
from html.parser import HTMLParser
from urllib.request import HTTPError, URLError
from constants import *
import threading


log = logging.Logger(name='Flat Server')
logging.basicConfig(level=logging.DEBUG)



all_urls = []
urls = []
answer = {}


class HtmlParser(HTMLParser):

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for name, value in attrs:
                if name == 'href':
                    if not (value.startswith("https://") or value.startswith("http://")
                    or value.startswith("file://") or value.startswith("ftp://")):
                        return
                    if value in all_urls:
                        return
                    if value not in urls:
                        urls.append(value)
                        all_urls.append(value)


class HTTPHandler(BaseHTTPRequestHandler):

    def __init__(self, request, client_address, server):
        super().__init__(request, client_address, server)

        #f = open("response.txt", 'w')
        #f.close()

    def __set_get_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'octet-stream')
        self.send_header('Content-Disposition', 'attachment; filename = "response.html"')
        self.end_headers()

    def __set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def __parse_query__(self, query):
        query_components = dict(qc.split('=') for qc in query.split('&'))
        search = query_components['search']
        depth = query_components['depth']
        domain = query_components['domain']
        return search, depth, domain


    def __get_html_response__(self, search, depth, domain):
        return self.server.get_html(url=search,depth=int(depth),domain=domain)


    def do_GET(self):
        query = urlparse(unquote(self.path)).query
        if len(query) > 0:

            self.__set_get_response()
            search, depth, domain = self.__parse_query__(query)
            logging.warning(f'GET request for url: {search} in domain: {domain} with depth: {depth}')
            html = self.__get_html_response__(search,depth,domain)

            s = '<!DOCTYPE html>' \
                '<html lang="en">' \
                '<head>' \
                '<meta charset="UTF-8">' \
                '<title>Distributed Scrapy</title>' \
                '</head>' \
                '<body>' \
                '<p>'
            items = ''
            for item in html:
                #print('!!!!!!!!!!!!!', item, '!!!!!!!!!!!!!!!!')
                items += "<h1>URL: " + str(item) + " HTML </h1> "
                items += str(html[item])
            r = '</p>' \
                '</form>' \
                '</body>' \
                '</html>'
            self.wfile.write(pickle.dumps(s + items + r))
        else:
            self.__set_response()
            s = '<!DOCTYPE html>' \
                '<html lang="en">' \
                '<head>' \
                '<meta charset="UTF-8">' \
                '<title>Distributed Scrapy</title>' \
                '</head>' \
                '<body>' \
                '<p>Text</p>' \
                '<form id="MyForm" method="get">' \
                'Search: <input type="url" name="search">' \
                'Domain: <input type="text" name="domain">' \
                'Depth: <input type="number" name="depth" min="1" max="5">' \
                '<input type="submit" value="Submit">' \
                '</form>' \
                '</body>' \
                '</html>'
            self.wfile.write(pickle.dumps(s))




class Broker(chordServer.node, FlatServer.Node, HTTPServer):
    def __init__(self, portin=5000, serveraddress=None, nbits=30, httpport=5002, handler=HTTPHandler):
        chordServer.node.__init__(self, nbits=nbits)

        host = socket.gethostname()
        host = socket.gethostbyname(host)
        address = (host,  httpport)
        log.warning(f'Running HTTP server in http://{host}:{httpport}')
        HTTPServer.__init__(self, address, handler)
        threading.Thread(target=self.serve_forever).start()
        FlatServer.Node.__init__(self, portin=portin, serveraddress=serveraddress)


    def manage_request(self, send_response, data):
        super().manage_request(send_response,data)

        code, *args = data
        if code == JOIN_GROUP:
            log.warning(f'received JOINGROUP request')
            address, udp_address = args
            id = self.registerNode(address, udp_address)
            send_response((id, self.NBits))
            msg = (ADD_GROUP, id, address, udp_address)
            self.broadcast(msg)

        if code == RANDOM_NODE:
            log.warning(f'received RANDOMNODE request')
            exceptions = args[0]
            node = self.getRandomNode(exceptions)
            if node is None:
                send_response((None, None, None))
            else:
                send_response((node.key, node.address, node.udp_address))

        if code == ADD_GROUP:
            id, address, udp_address = args
            log.warning(f'received AdToGroup for {id}')
            self.addToGroup(id, address, udp_address)

    def __scrap_urls__(self, url, domain, nivel):
        log.error("SCRAPPING....")
        urls.append(url)
        count = 1
        parser = HtmlParser()
        print(url, domain, nivel)
        while count <= nivel:
            level = urls.copy()
            urls.clear()
            for url in level:
                print(url)
                if not (url.startswith("https://") or url.startswith("http://")
                        or url.startswith("file://") or url.startswith("ftp://")):
                    print("URL ERROR")
                    continue
                if domain != '' and domain not in url:
                    print("DOMAIN ERROR")
                    continue
                html = self.__get_html_cache_nodes__(url)
                if html != "Empty" and not html is None:
                    log.warning("CACHE RETURNED HTML")
                    answer[url] = html
                    continue
                try:
                    log.error("SCRAPPING INTERNET")
                    r = urllib.request.urlopen(url)
                    print("1")
                    x = str(r.read())
                    print("2")
                    parser.feed(x)
                    print("3")
                    answer[url] = x
                    self.__save_html_cache__(url, x)
                except HTTPError as e:
                    log.error(e.getcode())
                except URLError as e:
                    log.error(e.reason)
            count += 1
        urls.clear()


    def __get_html_cache_nodes__(self, url):
        arr = [i for i in self.nodes]
        while len(arr) > 0:
            c = random.choice(arr)
            arr.remove(c)

            log.warning("Sending GET_URL" + url+ " to node:" + c.address)
            reply = self.ssocket_send((GET_URL, url), c)
            if reply is None:
                c.active = False
                continue
            print("REPLY  received ")
            return reply
        return "Empty"

    def __save_html_cache__(self, url, html):
        arr = [i for i in self.nodes]
        while len(arr) > 0:
            c = random.choice(arr)
            arr.remove(c)

            log.warning("Sending SAVE_HTML to node" + c.address )
            reply = self.ssocket_send((SAVE_URL, url, html), c)
            if reply is None or reply != ACK:
                c.active = False
                continue
            break


    def get_html(self, url, domain, depth):
        global answer
        answer.clear()
        log.warning("Scrapping URL" + url)
        self.__scrap_urls__(url=url, domain=domain, nivel=depth)
        return answer




def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--portin', type=int, default=5000, required=False,
                        help='Port for incoming communications on node')
    parser.add_argument('--address', type=str, required=False,
                        help='Address of node to connect to')
    parser.add_argument('--httpport', type=int, default=5002, required=False,
                        help='Port for HTTP server')
    parser.add_argument('--nbits', type=int, default=5, required=False,
                        help='Number of bits of the chord model')
    args = parser.parse_args()

    port1 = args.portin
    port3 = args.httpport
    nbits = args.nbits
    if args.address:
        host, port = args.address.split(':')
        address = (host, int(port))
        node = Broker(port1, address, nbits=nbits, httpport=port3)
    else:
        node = Broker(port1, nbits=nbits, httpport=port3)

    try:
        node.serve_forever()
    except KeyboardInterrupt:
        node.server_close()


if __name__ == '__main__':
    main()
