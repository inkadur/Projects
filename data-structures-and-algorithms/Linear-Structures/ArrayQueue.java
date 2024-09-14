import java.util.NoSuchElementException;

/**
 * Your implementation of an ArrayQueue.
 */
public class ArrayQueue<T> {

    /*
     * The initial capacity of the ArrayQueue.
     *
     * DO NOT MODIFY THIS VARIABLE.
     */
    public static final int INITIAL_CAPACITY = 9;

    /*
     * Do not add new instance variables or modify existing ones.
     */
    private T[] backingArray;
    private int front;
    private int size;

    /**
     * This is the constructor that constructs a new ArrayQueue.
     * 
     * Recall that Java does not allow for regular generic array creation,
     * so instead we cast an Object[] to a T[] to get the generic typing.
     */
    public ArrayQueue() {
        // DO NOT MODIFY THIS METHOD!
        backingArray = (T[]) new Object[INITIAL_CAPACITY];
    }

    /**
     * Adds the data to the back of the queue.
     *
     * If sufficient space is not available in the backing array, resize it to
     * double the current length. When resizing, copy elements to the
     * beginning of the new array and reset front to 0.
     *
     * Method should run in amortized O(1) time.
     *
     * @param data the data to add to the back of the queue
     * @throws java.lang.IllegalArgumentException if data is null
     */
    public void enqueue(T data) {
        if (data == null){
            throw new IllegalArgumentException();
        }
        // no resize necessary
        if (backingArray.length != size){
            backingArray[(front + size) % backingArray.length] = data;
        }
        // resize new array
        else{
            T[] newBackingArray = (T[]) new Object[backingArray.length*2];
            // copy data into new array starting from index 0
            for (int i = 0; i < size; i++){
                newBackingArray[i] = backingArray[(i+front+size)%size];
            }
            front = 0;
            // reset backing array to new one and add data to end
            backingArray = newBackingArray;
            backingArray[size] = data;
        }
        size++;
    }

    /**
     * Removes and returns the data from the front of the queue.
     *
     * Do not shrink the backing array.
     *
     * Replace any spots that you dequeue from with null.
     *
     * If the queue becomes empty as a result of this call, do not reset
     * front to 0.
     *
     * Method should run in O(1) time.
     *
     * @return the data formerly located at the front of the queue
     * @throws java.util.NoSuchElementException if the queue is empty
     */
    public T dequeue() {
        if (size == 0){
            throw new NoSuchElementException();
        }
        T deletedData = backingArray[front];
        backingArray[front] = null;
        front = front+1;
        // manually check if front is too big
        if (front == backingArray.length){
            front = 0;
        }
        size--;
        return deletedData;
    }

    /**
     * Returns the backing array of the queue.
     *
     * For grading purposes only. You shouldn't need to use this method since
     * you have direct access to the variable.
     *
     * @return the backing array of the queue
     */
    public T[] getBackingArray() {
        // DO NOT MODIFY THIS METHOD!
        return backingArray;
    }

    /**
     * Returns the size of the queue.
     *
     * For grading purposes only. You shouldn't need to use this method since
     * you have direct access to the variable.
     *
     * @return the size of the queue
     */
    public int size() {
        // DO NOT MODIFY THIS METHOD!
        return size;
    }

    public static void main(String[] args) {
        ArrayQueue<Integer> queue = new ArrayQueue<>();
        queue.enqueue(0);
        queue.enqueue(1);
        queue.enqueue(2);
        queue.enqueue(3);
        queue.enqueue(4);
        queue.enqueue(5);
        queue.enqueue(6);
        queue.enqueue(7);
        queue.enqueue(8);
        queue.enqueue(9);
        queue.enqueue(10);
        Object[] backingArray = queue.getBackingArray();
        for (int i = 0; i < backingArray.length; i++){
            System.out.print(backingArray[i]);
            System.out.print(",");
        }
        System.out.println();
        // queue.dequeue();
        // for (int i = 0; i < queue.backingArray.length; i++){
        //     System.out.print(queue.backingArray + ",");
        // }

        ArrayQueue<Integer> queue1 = new ArrayQueue<>();
        queue1.enqueue(0);
        queue1.enqueue(1);
        queue1.enqueue(2);
        queue1.enqueue(3);
        queue1.enqueue(4);
        queue1.enqueue(5);
        queue1.enqueue(6);
        queue1.dequeue();
        queue1.dequeue();
        queue1.enqueue(7);
        queue1.enqueue(8);
        queue1.enqueue(9);
        Object[] backingArray1 = queue1.getBackingArray();
        for (int i = 0; i < backingArray1.length; i++){
            System.out.print(backingArray1[i]);
            System.out.print(",");
        }
        

    }
}