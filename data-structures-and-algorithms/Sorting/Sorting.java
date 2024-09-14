
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Your implementation of various divide & conquer sorting algorithms.
 */

public class Sorting {

    /**
     * Implement merge sort.
     *
     * It should be:
     * out-of-place
     * stable
     * not adaptive
     *
     * Have a worst case running time of: O(n log n)
     * And a best case running time of: O(n log n)
     *
     * You can create more arrays to run merge sort, but at the end, everything
     * should be merged back into the original T[] which was passed in.
     *
     * When splitting the array, if there is an odd number of elements, put the
     * extra data on the right side.
     *
     * Hint: You may need to create a helper method that merges two arrays
     * back into the original T[] array. If two data are equal when merging,
     * think about which subarray you should pull from first.
     *
     * You may assume that the passed in array and comparator are both valid
     * and will not be null.
     *
     * @param <T>        Data type to sort.
     * @param arr        The array to be sorted.
     * @param comparator The Comparator used to compare the data in arr.
     */
    public static <T> void mergeSort(T[] arr, Comparator<T> comparator) {
        // base cases: length of arr is 1 or 0
        if (arr.length == 1){
            return;
        }
        if (arr.length == 0){
            return;
        }
        int mid = arr.length / 2;
        @SuppressWarnings("unchecked")
        T[] left = (T[]) new Object[mid];
        for (int i = 0; i< mid; i++){
            left[i] = arr[i];
        }
        @SuppressWarnings("unchecked")
        T[] right = (T[]) new Object[arr.length-mid];
        for (int i = 0; i < arr.length-mid; i++){
            right[i] = arr[mid+i];
        }
        mergeSort(left, comparator);
        mergeSort(right, comparator);
        mergeHelper(arr, left, right, comparator);
    }

    private static <T> void mergeHelper(T[] arr, T[] left, T[] right, Comparator<T> comparator){
        int i = 0;
        int j = 0;
        while (i < left.length && j < right.length){
            T lItem = left[i];
            T rItem = right[j];
            if(comparator.compare(lItem, rItem) <= 0){
                arr[i+j] = lItem;
                i++;
            }
            else{
                arr[i+j] = rItem;
                j++;
            }
        }
        while (i < left.length){
            arr[i+j] = left[i];
            i++;
        }
        while (j < right.length){
            arr[i+j] = right[j];
            j++;
        }
    }


    /**
     * Implement LSD (least significant digit) radix sort.
     *
     * It should be:
     * out-of-place
     * stable
     * not adaptive
     *
     * Have a worst case running time of: O(kn)
     * And a best case running time of: O(kn)
     *
     * Feel free to make an initial O(n) passthrough of the array to
     * determine k, the number of iterations you need.
     *
     * At no point should you find yourself needing a way to exponentiate a
     * number; any such method would be non-O(1). Think about how how you can
     * get each power of BASE naturally and efficiently as the algorithm
     * progresses through each digit.
     *
     * You may use an ArrayList or LinkedList if you wish, but it should only
     * be used inside radix sort and any radix sort helpers. Do NOT use these
     * classes with merge sort. However, be sure the List implementation you
     * choose allows for stability while being as efficient as possible.
     *
     * Do NOT use anything from the Math class except Math.abs().
     *
     * You may assume that the passed in array is valid and will not be null.
     *
     * @param arr The array to be sorted.
     */
    public static void lsdRadixSort(int[] arr) {
        @SuppressWarnings("unchecked")
        Queue<Integer>[] buckets = new LinkedList[19];
        int k = 0;
        for (int num : arr){
            int numDigits = String.valueOf(Math.abs(num)).length();
            if (numDigits > k){
                k = numDigits;
            }
        }
        for (int iteration = 0; iteration < k; iteration++){
            System.out.println("Iteration: " + iteration);
            for (int i = 0; i < arr.length; i++){
                int number = arr[i];
                int digit;
                int divisor = 1;
                for (int times = 1; times <= iteration; times++){
                    divisor = divisor * 10;
                }
                digit = Math.abs((number / (divisor)) % 10);
                if (number < 0){
                    digit = digit * -1;
                }
                System.out.print(number);
                System.out.print(" ");
                System.out.println(digit);
                if (buckets[digit+9] == null){
                    buckets[digit+9] = new LinkedList<>();
                }
                    buckets[digit+9].add(number);
            }
            int j = 0;
            for (Queue<Integer> bucket : buckets){
                while (bucket != null && bucket.peek() != null){
                    arr[j] = bucket.remove();
                    j++;
                }
            }
            for (int element : arr){
                System.out.print(element);
                System.out.print(',');
            }
            System.out.println();

            }
        }

    public static void main(String[] args) {
        int[] arr = {4321, -123, 543, 5, 90, 254, 15, 323, 1630, 434};
        lsdRadixSort(arr);
    }

}
