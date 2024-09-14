import java.util.NoSuchElementException;

/**
 * Implementation of a ExternalChainingHashMap.
 */
public class ExternalChainingHashMap<K, V> {

    /*
     * The initial capacity of the ExternalChainingHashMap when created with the
     * default constructor.
     *
     */
    public static final int INITIAL_CAPACITY = 13;

    /*
     * The max load factor of the ExternalChainingHashMap.
     *
     */
    public static final double MAX_LOAD_FACTOR = 0.67;


    private ExternalChainingMapEntry<K, V>[] table;
    private int size;

    /**
     * Constructs a new ExternalChainingHashMap with an initial capacity of INITIAL_CAPACITY.
     */
    @SuppressWarnings("unchecked")
    public ExternalChainingHashMap() {
        //DO NOT MODIFY THIS METHOD!
        table = (ExternalChainingMapEntry<K, V>[]) new ExternalChainingMapEntry[INITIAL_CAPACITY];
    }

    /**
     * Adds the given key-value pair to the map. If an entry in the map
     * already has this key, replace the entry's value with the new one
     * passed in.
     *
     * In the case of a collision, use external chaining as your resolution
     * strategy. Add new entries to the front of an existing chain, but don't
     * forget to check the entire chain for duplicate keys first.
     *
     * If you find a duplicate key, then replace the entry's value with the new
     * one passed in. When replacing the old value, replace it at that position
     * in the chain, not by creating a new entry and adding it to the front.
     *
     * Before actually adding any data to the HashMap, you should check to
     * see if the table would violate the max load factor if the data was
     * added. Resize if the load factor (LF) is greater than max LF (it is
     * okay if the load factor is equal to max LF). For example, let's say
     * the table is of length 5 and the current size is 3 (LF = 0.6). For
     * this example, assume that no elements are removed in between steps.
     * If another entry is attempted to be added, before doing anything else,
     * you should check whether (3 + 1) / 5 = 0.8 is larger than the max LF.
     * It is, so you would trigger a resize before you even attempt to add
     * the data or figure out if it's a duplicate. Be careful to consider the
     * differences between integer and double division when calculating load
     * factor.
     *
     * When regrowing, resize the length of the backing table to
     * (2 * old length) + 1. You should use the resizeBackingTable method to do so.
     *
     * @param key   The key to add.
     * @param value The value to add.
     * @return null if the key was not already in the map. If it was in the
     *         map, return the old value associated with it.
     * @throws java.lang.IllegalArgumentException If key or value is null.
     */
    public V put(K key, V value) {
        if (key == null || value == null){
            throw new IllegalArgumentException();
        }
        size++;
        V deletedValue = null;
        // check if you have to resize the table
        if (((float)size / (float)table.length) > MAX_LOAD_FACTOR){
            int newLength = (2 * table.length) + 1;
            resizeBackingTable(newLength);            
        }
        int location = hash(key, table.length);        
        ExternalChainingMapEntry<K, V> current = table[location];
        // if the location is empty, add it there
        if (current == null){
            ExternalChainingMapEntry<K, V> entry = new ExternalChainingMapEntry<>(key, value);
            table[location] = entry;            
        }
        else{
            // if the location is not empty, search to see if its in there
            boolean found = false;
            while (current != null && !found){
                if (current.getKey().equals(key)){
                    // if you find a match, save the old value and overwrite it
                    found = true;
                    deletedValue = current.getValue();
                    current.setValue(value);
                }
                // go to the next entry in the linked list
                current = current.getNext();                
        }
            if (!found){
                // if you never found it, make a new entry as the head of the linked list
                ExternalChainingMapEntry<K, V> entry = new ExternalChainingMapEntry<>(key, value, table[location]);
                table[location] = entry;
            }       
        }
        return deletedValue;
    }
    
    private int hash(K key, int length){
        int hash = key.hashCode();
        int location = Math.abs(hash % length);
        return location;
    }
    
    /**
     * Removes the entry with a matching key from the map.
     *
     * @param key The key to remove.
     * @return The value associated with the key.
     * @throws java.lang.IllegalArgumentException If key is null.
     * @throws java.util.NoSuchElementException   If the key is not in the map.
     */
    public V remove(K key) {
        if (key == null){
            throw new IllegalArgumentException();
        }
        int location = hash(key, table.length); 
        ExternalChainingMapEntry<K, V> current = table[location];
        // if the location is empty, add it there
        if (current == null){
            throw new NoSuchElementException();
        }
        V value = null;
        // if the location is not empty, search to see if its in there
        boolean found = false;
        if (current.getKey().equals(key)){
            found = true;
            value = current.getValue();
            table[location] = current.getNext();
        }
        
        while (!found && current.getNext() != null){
            if (current.getNext().getKey() == key){
                found = true;
                value = current.getNext().getValue();
                current.setNext(current.getNext().getNext());
            }
            // go to the next entry in the linked list
            current = current.getNext();                
        }
        if (value == null){
            throw new NoSuchElementException();
        }
        size--;                        
        return value;
        
    }

    /**
     * Helper method stub for resizing the backing table to length.
     *
     * This method should be called in put() if the backing table needs to
     * be resized.
     *
     * You should iterate over the old table in order of increasing index and
     * add entries to the new table in the order in which they are traversed.
     *
     * Since resizing the backing table is working with the non-duplicate
     * data already in the table, you won't need to explicitly check for
     * duplicates.
     *
     * Hint: You cannot just simply copy the entries over to the new table.
     *
     * @param Length The new length of the backing table.
     */
    private void resizeBackingTable(int length) {
        ExternalChainingMapEntry<K, V>[] newTable = (ExternalChainingMapEntry<K, V>[]) new ExternalChainingMapEntry[length];
        for (ExternalChainingMapEntry<K, V> element : this.table){
            if (element != null){
                add(element, newTable);
                while (element.getNext() != null){
                    element = element.getNext();
                    add(element, newTable);
                }
            }
        }
        this.table = newTable;
    }
    
    private void add(ExternalChainingMapEntry<K, V> element, ExternalChainingMapEntry<K, V>[] newTable){
        K key = element.getKey();
        V value = element.getValue();
        int location = hash(key, newTable.length); 
        ExternalChainingMapEntry<K, V> current = newTable[location];
        ExternalChainingMapEntry<K, V> entry;
        if (current == null){
            entry = new ExternalChainingMapEntry<>(key, value);
        }
        else{
            entry = new ExternalChainingMapEntry<>(key, value, current);
        }
        newTable[location] = entry;
    }

    

    /**
     * Returns the table of the map.
     *
     * For grading purposes only. You shouldn't need to use this method since
     * you have direct access to the variable.
     *
     * @return The table of the map.
     */
    public ExternalChainingMapEntry<K, V>[] getTable() {
        return table;
    }

    /**
     * Returns the size of the map.
     *
     * For grading purposes only. You shouldn't need to use this method since
     * you have direct access to the variable.
     *
     * @return The size of the map.
     */
    public int size() {
        return size;
    }

    public static void main(String[] args) {
        ExternalChainingHashMap<Integer, Integer> hash = new ExternalChainingHashMap<>();
        hash.put(0, 0);
        hash.put(1, 0);
        hash.put(2, 0);
        hash.put(3, 0);
        hash.put(4, 0);
        hash.put(5, 0);
        hash.put(27, 0);
        hash.put(14, 0);
        hash.remove(5);

        for (ExternalChainingMapEntry<Integer, Integer> entry : hash.getTable()){
            if (entry != null){
                System.out.println(entry.getKey());
            }
            else{
               System.out.println("null"); 
            }
            
        }
    }


    }

