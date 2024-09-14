package Sorting;

import org.junit.Assert;
import org.junit.Test;

import Sorting;

public class SortingTest {
    @Test
    public void testLSD(){
        int [] expected = {5, 15, 90, 123, 254, 323, 434, 543, 1630, 4321};
        int[] arr = {4321, 123, 543, 5, 90, 254, 15, 323, 1630, 434};
        Sorting.lsdRadixSort(arr);
        Assert.assertArrayEquals(expected, arr);
    }
    
}
