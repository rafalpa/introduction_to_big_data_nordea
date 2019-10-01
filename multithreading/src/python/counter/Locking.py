import threading
import time

N = 1000000
counter = 0

def increment(theLock):
    global counter
    for i in range(N):
        theLock.acquire()
        counter += 1
        theLock.release()

lock = threading.Lock()
t1 = threading.Thread(target=increment, args=[lock,])
t2 = threading.Thread(target=increment, args=[lock,])

#start = time.time()

t1.start()
t2.start()
t1.join()
t2.join()

#end = time.time()
#print(end - start)

print(counter)
