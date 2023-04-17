import queue
import random
import threading
import time

import concurrent.futures
import logging
from turtle import exitonclick
logging.basicConfig(level=logging.DEBUG, format='%(threadName)s: %(message)s')


symbols = ['A', 'B', 'C', ]
timeFrame = ['15m', '1h']
keys = [(symbol, timeframe) for symbol in symbols for timeframe in timeFrame]

# triggers = { key: threading.Event() for key in keys}


executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)


def worker(task_queue: queue.Queue, ):
    logging.debug(f'{threading.current_thread().daemon}')
    while True:
        task = task_queue.get(block=True)
        executor.submit(handle_task, task)


def handle_task(task):
    logging.debug(f'{threading.current_thread().daemon})')
    logging.debug(f'handle task {task}')
    time.sleep(random.randit(1, 3))


def add_task(task_queue: queue.Queue, task):
    time.sleep(5)  # wait for 5 seconds
    task_queue.put(task)  # add new task
    logging.debug(f'added new task: {task}')


task_queue = queue.Queue()
t1 = threading.Thread(target=worker, args=(task_queue, ))


def configure_tasks():
    for key in keys:
        # triggers[key].set()
        add_task(task_queue, key)


t2 = threading.Thread(target=configure_tasks,)

t1.start()
t2.start()

print('non-blocking main thread')

for i in range(1, 3):
    time.sleep(random.randint(1, 3))
# check if the thread is still alive
add_task(task_queue, 'new task')
# t1.join()
print('main thread finished, but the worker thread is still alive')
