from threading import Thread, Condition, Lock
import time
import random

queue = []
lock = Lock()
condition = Condition()

class ConsumerThread(Thread):
    def run(self):
        global queue
        while True:
            condition.acquire()
            if not queue:
                print ("Nothing in queue, consumer is waiting")
                condition.wait()
                print ("Producer added something to queue and notified the consumer")
            num = queue.pop(0)
            print ("Consumed", num)
            condition.release()
            time.sleep(random.random())


class ProducerThread(Thread):
    def run(self):
        nums = range(5)
        global queue
        while True:
            condition.acquire()
            num = random.choice(nums)
            queue.append(num)
            print ("Produced", num)
            print ("queue: ", queue)
            condition.notify()
            condition.release()
            time.sleep(random.random())


ProducerThread().start()
ProducerThread().start()
ConsumerThread().start()