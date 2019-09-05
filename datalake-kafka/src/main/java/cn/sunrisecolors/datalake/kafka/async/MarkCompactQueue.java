package cn.sunrisecolors.datalake.kafka.async;


import cn.sunrisecolors.datalake.kafka.RecordContext;

/**
 * 带标记的压缩队列
 *
 * @author shaohongliang
 * @since 2019/8/8 14:36
 */
public class MarkCompactQueue {
    private int front;//指向队首
    private int rear;//指向队尾C
    private AsyncRecordContext[] elem;
    private int maxSize;//最大容量
    private int allowSlideWindowIndex = -1;
    private Object obj = new Object(); // 阻塞锁

    public MarkCompactQueue(int maxSize) {
        this.elem = new AsyncRecordContext[maxSize];
        this.maxSize = maxSize;
        this.front = 0;
        this.rear = 0;
    }

    public boolean isFull() {
        return rear + 1 == front;
    }

    public void put(AsyncRecordContext recordContext) {
        while (isFull()) {
            // 如果已满，尝试压缩
            if (!compact()) {
                synchronized (obj) {
                    try {
                        obj.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        elem[rear] = recordContext;
        if (++rear == maxSize)
            rear = 0;
    }

    public RecordContext allowSlideWindowRecord() {
        RecordContext record = null;

        int start = front;
        int end = rear < front ? rear + maxSize : rear;

        // 当rear进入到末尾的时候，需要判断两次
        for (int i = start; i < end; i++) {
            int index = i >= maxSize ? maxSize - i : i;
            if (elem[index].isProcessed()) {
                record = elem[index];
                this.allowSlideWindowIndex = index;
            } else {
                break;
            }
        }

        return record;
    }

    public void slideWindow() {
        if (allowSlideWindowIndex > 0) {
            this.front = allowSlideWindowIndex;
            allowSlideWindowIndex = -1;
        }
    }

    /**
     * @return 压缩成功与否
     */
    private boolean compact() {
        int start = front;
        int end = rear < front ? rear + maxSize : rear;

        boolean lastProcessed = false;
        int compactedLen = 0;
        // 当rear进入到末尾的时候，需要判断两次
        for (int i = end; i > start; i--) {
            int index = i >= maxSize ? maxSize - i : i;
            if (elem[index].isProcessed()) {
                if (lastProcessed) {
                    // 如果上一个也是标记的，可压缩数量+1
                    compactedLen++;
                }
                lastProcessed = true;
            } else {
                lastProcessed = false;
                elem[index + compactedLen] = elem[index];
            }
        }

        if (compactedLen > 0) {
            front += compactedLen;
            if (front >= maxSize) {
                front -= maxSize;
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * 标记发生变化，唤醒put方法的阻塞
     */
    public void tigger() {
        synchronized (obj) {
            obj.notify();
        }
    }
}
