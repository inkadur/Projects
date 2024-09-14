import java.util.NoSuchElementException;

/**
 * Your implementation of a BST.
 */
public class BST<T extends Comparable<? super T>> {

    /*
     * Do not add new instance variables or modify existing ones.
     */
    private BSTNode<T> root;
    private int size;

    /*
     * Do not add a constructor.
     */

    /**
     * Adds the data to the tree.
     *
     * This must be done recursively.
     *
     * The new data should become a leaf in the tree.
     *
     * Traverse the tree to find the appropriate location. If the data is
     * already in the tree, then nothing should be done (the duplicate
     * shouldn't get added, and size should not be incremented).
     *
     * Should be O(log n) for best and average cases and O(n) for worst case.
     *
     * @param data The data to add to the tree.
     * @throws java.lang.IllegalArgumentException If data is null.
     */
    public void add(T data) {
        if (data == null){
            throw new IllegalArgumentException();
        }
        root = rAdd(root, data);
    }

    private BSTNode<T> rAdd(BSTNode<T> node, T data){
        // base case: the node is null
        if (node == null){
            size++;
            return new BSTNode<T>(data);
        }
        else {
            if (node.getData().compareTo(data) > 0){
                node.setLeft(rAdd(node.getLeft(), data));
            }
            if (node.getData().compareTo(data) < 0){
                node.setRight(rAdd(node.getRight(), data));
            }
            return node;
        }
    }

    /**
     * Removes and returns the data from the tree matching the given parameter.
     *
     * This must be done recursively.
     *
     * There are 3 cases to consider:
     * 1: The node containing the data is a leaf (no children). In this case,
     * simply remove it.
     * 2: The node containing the data has one child. In this case, simply
     * replace it with its child.
     * 3: The node containing the data has 2 children. Use the SUCCESSOR to
     * replace the data. You should use recursion to find and remove the
     * successor (you will likely need an additional helper method to
     * handle this case efficiently).
     *
     * Do NOT return the same data that was passed in. Return the data that
     * was stored in the tree.
     *
     * Hint: Should you use value equality or reference equality?
     *
     * Must be O(log n) for best and average cases and O(n) for worst case.
     *
     * @param data The data to remove.
     * @return The data that was removed.
     * @throws java.lang.IllegalArgumentException If data is null.
     * @throws java.util.NoSuchElementException   If the data is not in the tree.
     */
    public T remove(T data) {
        if (data == null){
            throw new IllegalArgumentException();
        }
        BSTNode<T> dummy = new BSTNode<T>(null);
        root = rRemove(root, data, dummy);
        return dummy.getData();
    }

    private BSTNode<T> rRemove(BSTNode<T> node, T data, BSTNode<T> dummy){
        // data not found
        if (node == null){
            throw new NoSuchElementException();
        }
        // data i am searching for is greater than current node i am on
        else if (node.getData().compareTo(data) < 0){
            node.setRight(rRemove(node.getRight(), data, dummy));
        }
        // data i am searching for is less than current node i am on
        else if (node.getData().compareTo(data) > 0){
            node.setLeft(rRemove(node.getLeft(), data, dummy));
        }
        // i found the data
        else{
            dummy.setData(node.getData());
            size--;
            // no children
            if (node.getRight() == null && node.getLeft() == null){
                return null;
            }
            // one child
            else if (node.getRight() == null){
                return node.getLeft();
            }
            else if (node.getLeft() == null){
                return node.getRight();
            }
            // two children
            else{
                BSTNode<T> dummy2 = new BSTNode<T>(null);
                node.setRight(rSuccessor(node.getRight(), dummy2));
                node.setData(dummy2.getData());
            }
        }
    return node;
    }

    private BSTNode<T> rSuccessor(BSTNode<T> node, BSTNode<T> dummy){
        if (node.getLeft() == null){
            dummy.setData(node.getData());
            return node.getRight();
        }
        else{
            node.setLeft(rSuccessor(node.getLeft(), dummy));
        }
        return node;
        
    }

    /**
     * Returns the root of the tree.
     *
     * For grading purposes only. You shouldn't need to use this method since
     * you have direct access to the variable.
     *
     * @return The root of the tree
     */
    public BSTNode<T> getRoot() {
        // DO NOT MODIFY THIS METHOD!
        return root;
    }

    /**
     * Returns the size of the tree.
     *
     * For grading purposes only. You shouldn't need to use this method since
     * you have direct access to the variable.
     *
     * @return The size of the tree
     */
    public int size() {
        // DO NOT MODIFY THIS METHOD!
        return size;
    }
}