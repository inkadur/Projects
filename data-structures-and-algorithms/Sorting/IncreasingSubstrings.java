package Sorting;
import java.util.Arrays;
import java.util.ArrayList;


public class IncreasingSubstrings{
    private int[] array;
    private int numSubstrings;

    public IncreasingSubstrings(int[] array){
        this.array = array;
        this.numSubstrings = findIncreasingSubstrings(this.array, this.array.length-1, new int[this.array.length]);
    }

    private int findIncreasingSubstrings(int[] array, int position, int[] numSubstrings){
        // base case- we are considering the entire array
        if (position == 0){
            int sum = 1;
            for (int i : numSubstrings){
                sum += i;
            } 
            numSubstrings[position] = sum;
            int totalSum = 0;
            for (int i : numSubstrings){
                totalSum += i;
            }
            return totalSum; 
        }
        else{
            ArrayList<Integer> greaterIndices = new ArrayList<>();
            for (int i = position+1; i < this.array.length; i++){
                if (array[i] > array[position]){
                    greaterIndices.add(i);
                }
            }
            int sum = 1;
            for (int index : greaterIndices){
                sum += numSubstrings[index];
            }
            numSubstrings[position] = sum;
            for (int num : numSubstrings){
                System.out.print(num);
                System.out.print(',');
            }
            System.out.println();
            return findIncreasingSubstrings(array, position-1, numSubstrings);
        }
    }

    public int getIncreasingSubstrings(){
        return this.numSubstrings;
    }


    public static void main(String[] args) {
        int[] testArray = {1, 7, 3, 5, 2};
        IncreasingSubstrings testSubstrings = new IncreasingSubstrings(testArray);
        System.out.println(testSubstrings.getIncreasingSubstrings());  
    }


}
