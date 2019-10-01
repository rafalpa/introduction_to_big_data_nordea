import threading
import time

N = 1000000
counter = 0

def increment():
    global counter
    for i in range(N):
        counter += 1

t1 = threading.Thread(target=increment)
t2 = threading.Thread(target=increment)

#start = time.time()
t1.start()
t2.start()
t1.join()
t2.join()
#end = time.time()
#print(end - start)
print(counter)
