# !usr/bin/python

import os, socket, subprocess, sys, time, signal

server = None
caches = []
cachesfd = []

try:
    host = socket.gethostname()
    host = socket.gethostbyname(host)
    #print(f'Connect browser to {host}:5001')

    server = subprocess.Popen(f'{sys.executable} HttpServer.py', shell=True)

    time.sleep(5)
    for i in range(5):
        f = open(f'cache{i}.txt', 'w+')
        cachesfd.append(f)
        caches.append(subprocess.Popen(f'{sys.executable} cache.py -ns {host}:5000', shell=True, stdout=f, stderr=f))
        time.sleep(2)

    while True: pass

except KeyboardInterrupt as e:
    #server.terminate()
    if server:
        os.kill(server.pid,signal.SIGKILL)
    for i in caches:
        #i.terminate()
        os.kill(i.pid, signal.SIGKILL)