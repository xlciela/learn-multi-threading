'''
SLEEP
'''

import time
import threading


def do_task(delay: int):
    print('foo is sleeping')
    time.sleep(delay)
    print('foo wakes up')


th_foo = threading.Thread(target=lambda: do_task(3))

th_foo.start()
th_foo.join()

print('Good bye')
