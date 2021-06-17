from http.server import BaseHTTPRequestHandler, HTTPServer
import socket
from urllib.parse import urlparse, unquote
import urllib.request
from html.parser import HTMLParser
from urllib.request import HTTPError, URLError
import threading
from broker import Broker
from logFormatter import logger


class HtmlParser(HTMLParser):

    def __init__(self):
        super().__init__()
        self.urls = []
        self.all_urls = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for name, value in attrs:
                if name == 'href':
                    if not (value.startswith("https://") or value.startswith("http://")
                            or value.startswith("file://") or value.startswith("ftp://")):
                        return
                    if value in self.all_urls:
                        return
                    if value not in self.urls:
                        self.urls.append(value)
                        self.all_urls.append(value)


class HTTPHandler(BaseHTTPRequestHandler):

    def __init__(self, request, client_address, server):
        super().__init__(request, client_address, server)

        # f = open("response.txt", 'w')
        # f.close()

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
        return self.server.get_html(url=search ,depth=int(depth) ,domain=domain)


    def do_GET(self):
        query = urlparse(unquote(self.path)).query
        if len(query) > 0:

            self.__set_get_response()
            search, depth, domain = self.__parse_query__(query)
            logger.info(f'GET request for url: {search} in domain: {domain} with depth: {depth}')
            html = self.__get_html_response__(search ,depth ,domain)

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
                # print('!!!!!!!!!!!!!', item, '!!!!!!!!!!!!!!!!')
                items += "<h1 style = 'color: cornflowerblue'>URL: " + str(item) + " </h1> "
                items += str(html[item])
            r = '</p>' \
                '</form>' \
                '</body>' \
                '</html>'
            try:
                self.wfile.write((s + items + r).encode('utf-8'))
            except BrokenPipeError as e:
                logger.error(f'ERROR {e.errno}: {e.strerror}')
        else:
            self.__set_response()
            s = '<!DOCTYPE html>' \
                '<html lang="en">' \
                '<head>' \
                '<meta charset="UTF-8">' \
                '<title>Distributed Scrapy</title>' \
                '</head>' \
                '<body>' \
                '<h2>Insert URL to scrap</h2>' \
                '<form id="MyForm" method="get">' \
                'URL: <input style="margin-right: 20px" required = "true"  type="url" name="search">' \
                'Domain: <input style="margin-right: 20px" type="text" name="domain">' \
                'Depth: <input style="margin-right: 20px" required = "true" type="number" name="depth" min="1" max="5">' \
                '<input type="submit" value="Submit">' \
                '</form>' \
                '</body>' \
                '</html>'
            try:
                self.wfile.write(s.encode('utf-8'))
            except BrokenPipeError as e:
                logger.error(f'ERROR {e.errno}: {e.strerror}')

class HttpServer(HTTPServer):
    def __init__(self, broker, port=5001, handler=HTTPHandler):
        self.broker = broker
        host = socket.gethostname()
        host = socket.gethostbyname(host)
        address = (host, port)
        logger.info(f'Running HTTP server in http://{host}:{port}')
        HTTPServer.__init__(self, address, handler)

    def get_html(self, url, domain, depth):
        answer = self.__scrap_urls__(url=url, domain=domain, nivel=depth)
        return answer

    def __scrap_urls__(self, url, domain, nivel):
        answer = {}
        parser = HtmlParser()
        parser.urls.append(url)
        print(f'INITIAL  {parser.urls}, DEPTH {nivel}')
        count = 1
        print(url, domain, nivel)
        while count <= nivel:
            level = parser.urls.copy()
            print(f'LEVEL {count}  {parser.urls}')
            for url in level:
                logger.info(f"Getting html for {url}")
                print(url)
                if not (url.startswith("https://") or url.startswith("http://")
                        or url.startswith("file://") or url.startswith("ftp://")):
                    print("URL ERROR")
                    continue
                if domain != '' and domain not in url:
                    answer[url] = "DOMAIN ERROR"
                    continue
                html = self.broker.__get_cache_data__(url)
                if html != "Empty" and not html is None:
                    logger.info("CACHE RETURNED HTML")
                    answer[url] = html
                    if count < nivel:
                        parser.feed(html)
                else:
                    try:
                        logger.info("Scrapping internet")
                        with urllib.request.urlopen(url, timeout=15) as r:
                            # if r.getcode() != 200:
                            # answer[url] = "GET request returned code " + r.getcode()
                            #    continue
                            print("1")
                            x = r.read().decode('utf-8')
                            print("2")
                        parser.feed(x)
                        print("3")
                        answer[url] = x
                        self.broker.__save_cache_data__(url, x)
                    except HTTPError as e:
                        answer[url] = "HTTP ERROR" + str(e.getcode())
                        logger.error(e.getcode())
                    except URLError as e:
                        answer[url] = "URL ERROR: " + str(e.reason)
                        logger.error(e.reason)
            count += 1
        return answer


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--portin', type=int, default=5000, required=False,
                        help='Port for incoming communications on node')
    parser.add_argument('-a', '--address', type=str, required=False,
                        help='Address of node to connect to')
    parser.add_argument('--httpport', type=int, default=5001, required=False,
                        help='Port for HTTP server')
    parser.add_argument('--nbits', type=int, default=5, required=False,
                        help='Number of bits of the chord model')
    args = parser.parse_args()

    port1 = args.portin
    port2 = args.httpport
    nbits = args.nbits
    if args.address:
        host, port = args.address.split(':')
        address = (host, int(port))
        node = Broker(port1, address, nbits=nbits)
    else:
        node = Broker(port1, nbits=nbits)

    server = HttpServer(node, port2)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()


if __name__ == '__main__':
    main()