import java.util.NoSuchElementException;

/**
 *Implementation of an AVL.
 */
public class AVL<T extends Comparable<? super T>> {

    
    private AVLNode<T> root;
    private int size;

    
    /**
     * Adds the element to the tree.
     *
     * Start by adding it as a leaf like in a regular BST and then rotate the
     * tree as necessary.
     *
     * If the data is already in the tree, then nothing should be done (the
     * duplicate shouldn't get added, and size should not be incremented).
     *
     * Remember to recalculate heights and balance factors while going back
     * up the tree after adding the element, making sure to rebalance if
     * necessary. This is as simple as calling the balance() method on the
     * current node, before returning it (assuming that your balance method
     * is written correctly from part 1 of this assignment).
     *
     * @param data The data to add.
     * @throws java.lang.IllegalArgumentException If data is null.
     */
    public void add(T data) {
        if (data == null){
            throw new IllegalArgumentException();
        }
        root = rAdd(root, data);
    }

    private AVLNode<T> rAdd(AVLNode<T> node, T data){
        // base case: the node is null
        if (node == null){
            size++;
            return new AVLNode<T>(data);
        }
        else {
            if (node.getData().compareTo(data) > 0){
                node.setLeft(rAdd(node.getLeft(), data));
            }
            if (node.getData().compareTo(data) < 0){
                node.setRight(rAdd(node.getRight(), data));
            }
            // do the fun AVL stuff here!!
            return balance(node);
        }
    }

    /**
     * Removes and returns the element from the tree matching the given
     * parameter.
     *
     * There are 3 cases to consider:
     * 1: The node containing the data is a leaf (no children). In this case,
     *    simply remove it.
     * 2: The node containing the data has one child. In this case, simply
     *    replace it with its child.
     * 3: The node containing the data has 2 children. Use the successor to
     *    replace the data, NOT predecessor. As a reminder, rotations can occur
     *    after removing the successor node.
     *
     * Remember to recalculate heights and balance factors while going back
     * up the tree after removing the element, making sure to rebalance if
     * necessary. This is as simple as calling the balance() method on the
     * current node, before returning it (assuming that your balance method
     * is written correctly from part 1 of this assignment).
     *
     * Do NOT return the same data that was passed in. Return the data that
     * was stored in the tree.
     *
     * Hint: Should you use value equality or reference equality?
     *
     * @param data The data to remove.
     * @return The data that was removed.
     * @throws java.lang.IllegalArgumentException If the data is null.
     * @throws java.util.NoSuchElementException   If the data is not found.
     */
    public T remove(T data) {
        if (data == null){
            throw new IllegalArgumentException();
        }
        AVLNode<T> dummy = new AVLNode<T>(null);
        root = rRemove(root, data, dummy);
        return dummy.getData();
    }

    private AVLNode<T> rRemove(AVLNode<T> node, T data, AVLNode<T> dummy){
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
                    AVLNode<T> dummy2 = new AVLNode<T>(null);
                    node.setRight(rSuccessor(node.getRight(), dummy2));
                    node.setData(dummy2.getData());
                }
            }
        return balance(node);
    }

    private AVLNode<T> rSuccessor(AVLNode<T> node, AVLNode<T> dummy){
        if (node.getLeft() == null){
            dummy.setData(node.getData());
            return node.getRight();
        }
        else{
            node.setLeft(rSuccessor(node.getLeft(), dummy));
        }
        return balance(node);
    }



    /**
     * Updates the height and balance factor of a node using its
     * setter methods.
     *
     * Recall that a null node has a height of -1. If a node is not
     * null, then the height of that node will be its height instance
     * data. The height of a node is the max of its left child's height
     * and right child's height, plus one.
     *
     * The balance factor of a node is the height of its left child
     * minus the height of its right child.
     *
     * This method should run in O(1).
     * You may assume that the passed in node is not null.
     *
     * This method should only be called in rotateLeft(), rotateRight(),
     * and balance().
     *
     * @param currentNode The node to update the height and balance factor of.
     */
    private void updateHeightAndBF(AVLNode<T> node) {
        AVLNode<T> lChild = node.getLeft();
        AVLNode<T> rChild = node.getRight();
        int lHeight = (lChild == null ? -1 : lChild.getHeight());
        int rHeight = (rChild == null ? -1 : rChild.getHeight());
        int height = Math.max(lHeight, rHeight) + 1;
        int bf = lHeight - rHeight;
        node.setHeight(height);
        node.setBalanceFactor(bf);
    }

    /**
     * Method that rotates a current node to the left. After saving the
     * current's right node to a variable, the right node's left subtree will
     * become the current node's right subtree. The current node will become
     * the right node's left subtree.
     *
     * Don't forget to recalculate the height and balance factor of all
     * affected nodes, using updateHeightAndBF().
     *
     * This method should run in O(1).
     *
     * You may assume that the passed in node is not null and that the subtree
     * starting at that node is right heavy. Therefore, you do not need to
     * perform any preliminary checks, rather, you can immediately perform a
     * left rotation on the passed in node and return the new root of the subtree.
     *
     * This method should only be called in balance().
     *
     * @param currentNode The current node under inspection that will rotate.
     * @return The parent of the node passed in (after the rotation).
     */
    private AVLNode<T> rotateLeft(AVLNode<T> currentNode) {
        // save pointer to right child
        AVLNode<T> newRoot = currentNode.getRight();
        // set currNode right child to newRoot right child
        currentNode.setRight(newRoot.getLeft());
        // set roots left child to the current node
        newRoot.setLeft(currentNode);
        // update height and balence factor of current node
        updateHeightAndBF(currentNode);
        // update height and balence factor of new root
        updateHeightAndBF(newRoot);
        // return the new root of subtree
        return newRoot;
    }

    /**
     * Method that rotates a current node to the right. After saving the
     * current's left node to a variable, the left node's right subtree will
     * become the current node's left subtree. The current node will become
     * the left node's right subtree.
     *
     * Don't forget to recalculate the height and balance factor of all
     * affected nodes, using updateHeightAndBF().
     *
     * This method should run in O(1).
     *
     * You may assume that the passed in node is not null and that the subtree
     * starting at that node is left heavy. Therefore, you do not need to perform
     * any preliminary checks, rather, you can immediately perform a right
     * rotation on the passed in node and return the new root of the subtree.
     *
     * This method should only be called in balance().
     *
     * @param currentNode The current node under inspection that will rotate.
     * @return The parent of the node passed in (after the rotation).
     */
    private AVLNode<T> rotateRight(AVLNode<T> currentNode) {
        // set the left child as the new root
        AVLNode<T> newRoot = currentNode.getLeft();
        // set the currents left child to new roots right child
        currentNode.setLeft(newRoot.getRight());
        // set the new roots right to the current node
        newRoot.setRight(currentNode);
        // update height and bf of current node
        updateHeightAndBF(currentNode);
        // update height and hf of new root
        updateHeightAndBF(newRoot);
        // return the new root
        return newRoot;
    }

    /**
     * Method that balances out the tree starting at the node passed in.
     * This method should be called in your add() and remove() methods to
     * facilitate rebalancing your tree after an operation.
     *
     * The height and balance factor of the current node is first recalculated.
     * Based on the balance factor, a no rotation, a single rotation, or a
     * double rotation takes place. The current node is returned.
     *
     * You may assume that the passed in node is not null. Therefore, you do
     * not need to perform any preliminary checks, rather, you can immediately
     * check to see if any rotations need to be performed.
     *
     * This method should run in O(1).
     *
     * @param currentNode The current node under inspection.
     * @return The AVLNode that the caller should return.
     */
    private AVLNode<T> balance(AVLNode<T> currentNode) {
        /* First, we update the height and balance factor of the current node. */
        updateHeightAndBF(currentNode);

        if (currentNode.getBalanceFactor() < -1 /* Condition for a right heavy tree. */ ) {
            if (currentNode.getRight().getBalanceFactor() > 0 /* Condition for a right-left rotation. */ ) {
                currentNode.setRight(rotateRight(currentNode.getRight()));
            }
            currentNode = rotateLeft(currentNode);
        } else if (currentNode.getBalanceFactor() > 1 /* Condition for a left heavy tree. */ ) {
            if (currentNode.getLeft().getBalanceFactor() < 0 /* Condition for a left-right rotation. */ ) {
                currentNode.setLeft(rotateLeft(currentNode.getLeft()));
            }
            currentNode = rotateRight(currentNode);
        }

        return currentNode;
    }

    /**
     * Returns the root of the tree.
     *
     * For grading purposes only. You shouldn't need to use this method since
     * you have direct access to the variable.
     *
     * @return The root of the tree.
     */
    public AVLNode<T> getRoot() {
        // DO NOT MODIFY THIS METHOD!
        return root;
    }

    /**
     * Returns the size of the tree.
     *
     * For grading purposes only. You shouldn't need to use this method since
     * you have direct access to the variable.
     *
     * @return The size of the tree.
     */
    public int size() {
        // DO NOT MODIFY THIS METHOD!
        return size;
    }
}


/*
 * Update height and bf- based on child
 * rotate right (curNode)
 *      save pointer to cur left child (newRoot)
 *      set curr.leftChild to root.rightChild
 *      set root.rightChild to curr
 *      update height & bf of curr
 *      update height & bf of root
 *      return root
 * 
 * rotate left (curNode)
 *      save pointer to cur right child (newRoot)
 *      set curr.rightChild to root.leftChild
 *      set root.leftChild to curr
 *      update height & bf of curr
 *      update height & bf of root
 *      return root
 * 
 * balance
 *      update height and balence factor of the node you are on
 *      if (curr.bf < -1)       right heavy
 *          if (curr.right.bf > 0)    right child is left heavy
 *                  right rotation on right child first
 *                  (curr.setRight(rotateRight(curr.getRight)))
 *          rotate left on current node 
 *          cur = rotateLeft(curr)
 *      if (curr.bs > 1)        left heavy
 *          if (curr.left.bf < 0)       left child is right heavy
 *              curr.setLeft(rotateLeft(curr.getLeft))
 *          rotate right on current node
 *          cur = rotateRight(curr)
 *     return cur (if the node is balanced it just returns the node)
 * 
 * 
 * balence the tree for each node you are on as you return the node for pointer reinforcement
 */
