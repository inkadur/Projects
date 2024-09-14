package Sorting;
import java.util.Comparator;

/**
 * Your implementation of various iterative sorting algorithms.
 */
public class IterativeSorting {

    /**
     * Implement bubble sort.
     *
     * It should be:
     * in-place
     * stable
     * adaptive
     *
     * Have a worst case running time of: O(n^2)
     * And a best case running time of: O(n)
     *
     * NOTE: You should implement bubble sort with the last swap optimization.
     *
     * You may assume that the passed in array and comparator
     * are both valid and will never be null.
     *
     * @param <T>        Data type to sort.
     * @param arr        The array that must be sorted after the method runs.
     * @param comparator The Comparator used to compare the data in arr.
     */
    public static <T> void bubbleSort(T[] arr, Comparator<T> comparator) {
        // for the first go through we have to go all the way until the last index (so set the end to last index)
        int end = arr.length - 1;
        // initialize lastSwap for later use
        int lastSwap = 0;
        do {
            // for the next interation of the look only go until the last swap that was made (since the rest of the list is sorted)
            end = lastSwap;
            // reset the last swap to 0 at start for 
            lastSwap = 0;
            // loop through array from beginnging until we want to stop 
            for (int i = 0; i < end; i++) {
                if (comparator.compare(arr[i], arr[i+1]) > 0 ){
                    // if we need a swap, do it and set the lastSwap index to the index of the swap
                    T big = arr[i];
                    T small = arr[i+1];
                    arr[i] = small;
                    arr[i+1] = big;
                    lastSwap = i;
                }
            }
        } 
        // do while lets us go through list at least once and then only as needed
        while((lastSwap != 0));
        
    }

    /**
     * Implement selection sort.
     *
     * It should be:
     * in-place
     * unstable
     * not adaptive
     *
     * Have a worst case running time of: O(n^2)
     * And a best case running time of: O(n^2)
     *
     * You may assume that the passed in array and comparator
     * are both valid and will never be null.
     *
     * @param <T>        Data type to sort.
     * @param arr        The array that must be sorted after the method runs.
     * @param comparator The Comparator used to compare the data in arr.
     */
    public static <T> void selectionSort(T[] arr, Comparator<T> comparator) {
        // go through list to find max, swap the max with that index
        for (int outer = arr.length - 1; outer > 0; outer--){
            int maxIdx = 0;
            T max = arr[maxIdx];
            for (int inner = 1; inner <= outer; inner++){
                // if the node i am on is larger than the max value i have stored
                if (comparator.compare(arr[inner], max) > 0){
                    maxIdx = inner;
                    max = arr[inner];
                } 
            }
            // swap the max into right place
            arr[maxIdx] = arr[outer];
            arr[outer] = max;
        }
    }

    /**
     * Implement insertion sort.
     *
     * It should be:
     * in-place
     * stable
     * adaptive
     *
     * Have a worst case running time of: O(n^2)
     * And a best case running time of: O(n)
     *
     * You may assume that the passed in array and comparator
     * are both valid and will never be null.
     *
     * @param <T>        Data type to sort.
     * @param arr        The array that must be sorted after the method runs.
     * @param comparator The Comparator used to compare the data in arr.
     */
    public static <T> void insertionSort(T[] arr, Comparator<T> comparator) {
        // outer loop goes from 1 to the end
        for (int i = 1; i < arr.length; i++){
            // inner loop goes from i+1 until it stops swaping
            T value = arr[i];
            int location = i-1;
            while (location >= 0 && comparator.compare(value, arr[location]) < 0){
                arr[location+1] = arr[location];
                arr[location] = value;
                location--;
            }
        }
    }
}
