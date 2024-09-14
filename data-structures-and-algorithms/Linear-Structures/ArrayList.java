import java.util.NoSuchElementException;

/**
 * Your implementation of an ArrayList.
 */
public class ArrayList<T> {

    /*
     * The initial capacity of the ArrayList.
     *
     * DO NOT MODIFY THIS VARIABLE!
     */
    public static final int INITIAL_CAPACITY = 9;

    /*
     * Do not add new instance variables or modify existing ones.
     */
    private T[] backingArray;
    private int size;

    /**
     * This is the constructor that constructs a new ArrayList.
     * 
     * Recall that Java does not allow for regular generic array creation,
     * so instead we cast an Object[] to a T[] to get the generic typing.
     */
    public ArrayList() {
        //DO NOT MODIFY THIS METHOD!
        backingArray = (T[]) new Object[INITIAL_CAPACITY];
    }

    /**
     * Adds the data to the front of the list.
     *
     * This add may require elements to be shifted.
     *
     * Method should run in O(n) time.
     *
     * @param data the data to add to the front of the list
     * @throws java.lang.IllegalArgumentException if data is null
     */
    public void addToFront(T data) {
        if (data == null){
            throw new IllegalArgumentException();
        }
        // if the backingArray is at capacity, double the size and add new element
        else if (backingArray[backingArray.length-1] != null){
            T[] newBackingArray = (T[]) new Object[backingArray.length*2];
            newBackingArray[0] = data;
            for (int i = 0; i < size; i++){
                newBackingArray[i+1] = backingArray[i];
            }
            backingArray = newBackingArray;
            this.size++;
        }
        // else just shift all elements and add new one to index 0
        else{
            for (int i = size-1; i >= 0; i--){
                backingArray[i+1] = backingArray[i];
            }
            backingArray[0] = data;
            this.size++;
        }
    }

    /**
     * Adds the data to the back of the list.
     *
     * Method should run in amortized O(1) time.
     *
     * @param data the data to add to the back of the list
     * @throws java.lang.IllegalArgumentException if data is null
     */
    public void addToBack(T data) {
        if (data == null){
            throw new IllegalArgumentException();
        }
        // if backingArray is at capacity, resize to double, copy over all elements, and add new element 
        else if (backingArray[backingArray.length-1] != null){
            T[] newBackingArray = (T[]) new Object[backingArray.length*2];
            for (int i = 0; i < size; i++){
                newBackingArray[i] = backingArray[i];
            }
            backingArray = newBackingArray;
            backingArray[size] = data;
            this.size++;
        }
        // else just add the element to the back
        else{
            backingArray[size] = data;
            size++;
        }
    }

    /**
     * Removes and returns the first data of the list.
     *
     * Do not shrink the backing array.
     *
     * This remove may require elements to be shifted.
     *
     * Method should run in O(n) time.
     *
     * @return the data formerly located at the front of the list
     * @throws java.util.NoSuchElementException if the list is empty
     */
    public T removeFromFront() {
        if (backingArray[0] == null){
            throw new NoSuchElementException();
        }
        T deletedData = backingArray[0];
        for (int i = 0; i < size-1; i++){
            backingArray[i] = backingArray[i+1];
        }
        size--;
        return deletedData;

    }

    /**
     * Removes and returns the last data of the list.
     *
     * Do not shrink the backing array.
     *
     * Method should run in O(1) time.
     *
     * @return the data formerly located at the back of the list
     * @throws java.util.NoSuchElementException if the list is empty
     */
    public T removeFromBack() {
        if (backingArray[0] == null){
            throw new NoSuchElementException();
        }
        T deletedData = backingArray[size-1];
        backingArray[size-1] = null;
        size--;
        return deletedData;
    }

    /**
     * Returns the backing array of the list.
     *
     * For grading purposes only. You shouldn't need to use this method since
     * you have direct access to the variable.
     *
     * @return the backing array of the list
     */
    public T[] getBackingArray() {
        // DO NOT MODIFY THIS METHOD!
        return backingArray;
    }

    /**
     * Returns the size of the list.
     *
     * For grading purposes only. You shouldn't need to use this method since
     * you have direct access to the variable.
     *
     * @return the size of the list
     */
    public int size() {
        // DO NOT MODIFY THIS METHOD!
        return size;
    }

    public static void main(String[] args) {
        // ArrayList<Integer> list = new ArrayList<>();
        // System.out.println(list.size());
        // list.addToBack(20);
        // System.out.println(list.size());
        // list.addToFront(10);
        // System.out.println(list.size());
        // System.out.println(list.removeFromBack());
        // System.out.println(list.size());
        // System.out.println(list.removeFromFront());
        // System.out.println(list.size());

        ArrayList<Integer> list1 = new ArrayList<>();
        list1.addToBack(0);
        list1.addToBack(1);
        list1.addToBack(2);
        list1.addToBack(3);
        list1.addToBack(4);
        list1.addToBack(5);
        list1.addToBack(6);
        list1.addToBack(7);
        list1.addToBack(8);
        list1.addToFront(9);     
    }
}
