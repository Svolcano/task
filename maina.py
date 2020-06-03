from multiprocessing import Pipe, Process, Event
import time
from datetime import datetime
import random
import re

stop = Event()
stop.clear()

def producer(end, sleep_time = 1):
    while True:
        data = random.randint(1, 10000)
        print('send', data)
        end.send("{}:rand a number, be {}".format(datetime.now(), data))
        time.sleep(sleep_time)
        if stop.is_set():
            print("producer over")
            end.close()
            break


def consumer(end):
    c = 0
    while True:
        c += 1
        try:
            data = end.recv()
            groups = re.search(r'(\d+)$', data)
            if groups:
                number = groups.groups()[0]
                print("index:{}, got: {}".format(c, number))
            else:
                print('index:{}, something wrong!'.format(c))
        except EOFError as e:
            print("connection had broken")


        if stop.is_set():
            print('consumer over')
            end.close()
            break


p, q = Pipe()

# p.recv()
p = Process(target=producer, args=(p,))
p.daemon = True
p.start()

q = Process(target=consumer, args=(q,))
q.daemon = True
q.start()

c = 10
while c :
    c -= 1
    time.sleep(0.2)
stop.set()


p.join()
q.join()
