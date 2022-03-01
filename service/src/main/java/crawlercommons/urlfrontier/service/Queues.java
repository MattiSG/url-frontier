package crawlercommons.urlfrontier.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In the absence of a ConcurrentLinkedHashMap, we use a custom data-structure which allows to
 * rotate elements from head to tail but also provide direct access by key.
 */
public class Queues {

    private final List<QueueWithinCrawl> queue;
    private final ConcurrentHashMap<QueueWithinCrawl, QueueInterface> content;
    private int position;

    public Queues() {
        this.queue = new ArrayList<>();
        this.content = new ConcurrentHashMap<>();
        this.position = 0;
    }

    public QueueInterface get(QueueWithinCrawl key) {
        return content.get(key);
    }

    public boolean containsKey(QueueWithinCrawl key) {
        return content.containsKey(key);
    }

    public QueueInterface remove(QueueWithinCrawl key) {
        synchronized (queue) {
            QueueInterface val = content.remove(key);
            queue.remove(key);
            return val;
        }
    }

    /**
     * Copies the keys to allow iterating on the content without the need to lock on the queues.
     * Since this is a copy, the content might no reflect the latest changes. *
     */
    public QueueWithinCrawl[] getKeys() {
        return queue.toArray(new QueueWithinCrawl[0]);
    }

    public QueueInterface putIfAbsent(QueueWithinCrawl key, QueueInterface value) {
        synchronized (queue) {
            QueueInterface v = content.putIfAbsent(key, value);
            // has been created?
            // add it to the back of the chain
            if (v == null) {
                queue.add(key);
            }
            return v;
        }
    }

    public int size() {
        return content.size();
    }

    public QueueWithinCrawl getNextKey() {
        synchronized (queue) {
            // no elements?
            if (queue.isEmpty()) {
                return null;
            }
            return queue.get(position);
        }
    }

    public void moveToNext() {
        synchronized (queue) {
            position++;
            if (position == queue.size()) position = 0;
        }
    }
}
