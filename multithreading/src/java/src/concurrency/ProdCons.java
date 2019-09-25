package concurrency;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ProdCons {
    public static void main(String[] args) throws InterruptedException {
        Storage storage = new Storage();
        Thread c = new Thread() {
            public void run() {
                for (int i = 0; i<10; i++) {
                    System.out.println("Konsument pobral " + storage.get());
                }
            }
        };
        Thread p = new Thread() {
            public void run() {
                for (int i = 0; i<10; i++) {
                    storage.put(i);
                    System.out.println("Producent wyprodukowal " + i);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                    }
                }
            }
        };

        c.start();
        p.start();
    }
}

class Storage {

    BlockingQueue bq = new LinkedBlockingQueue();

    public Integer get()  {
        try {
            return (Integer) bq.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void put(Integer value)  {
        try {
            bq.put(value);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}