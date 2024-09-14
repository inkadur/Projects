import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Your implementation of the pre-order, in-order, and post-order
 * traversals of a tree.
 */
public class Traversals<T extends Comparable<? super T>> {

    /**
     * DO NOT ADD ANY GLOBAL VARIABLES!
     */

    /**
     * Given the root of a binary search tree, generate a
     * pre-order traversal of the tree. The original tree
     * should not be modified in any way.
     *
     * This must be done recursively.
     *
     * Must be O(n).
     *
     * @param <T> Generic type.
     * @param root The root of a BST.
     * @return List containing the pre-order traversal of the tree.
     */
    public List<T> preorder(TreeNode<T> root) {
        LinkedList<T> result = new LinkedList<>();
        preorderHelper(root, result);
        return result;
    }

    private void preorderHelper(TreeNode<T> node, LinkedList<T> currResult){
        if (node == null){
            return;
        }
        currResult.addLast(node.getData());
        preorderHelper(node.getLeft(), currResult);
        preorderHelper(node.getRight(), currResult);
    }

    /**
     * Given the root of a binary search tree, generate an
     * in-order traversal of the tree. The original tree
     * should not be modified in any way.
     *
     * This must be done recursively.
     *
     * Must be O(n).
     *
     * @param <T> Generic type.
     * @param root The root of a BST.
     * @return List containing the in-order traversal of the tree.
     */
    public List<T> inorder(TreeNode<T> root) {
        LinkedList<T> result = new LinkedList<>();
        inorderHelper(root, result);
        return result;
    }

    private void inorderHelper(TreeNode<T> node, LinkedList<T> currResult){
        if (node == null){
            return;
        }
        inorderHelper(node.getLeft(), currResult);
        currResult.addLast(node.getData());
        inorderHelper(node.getRight(), currResult);
    }

    /**
     * Given the root of a binary search tree, generate a
     * post-order traversal of the tree. The original tree
     * should not be modified in any way.
     *
     * This must be done recursively.
     *
     * Must be O(n).
     *
     * @param <T> Generic type.
     * @param root The root of a BST.
     * @return List containing the post-order traversal of the tree.
     */
    public List<T> postorder(TreeNode<T> root) {
        LinkedList<T> result = new LinkedList<>();
        postorderHelper(root, result);
        return result;    
    }

    private void postorderHelper(TreeNode<T> node, LinkedList<T> currResult){
        if (node == null){
            return;
        }
        postorderHelper(node.getLeft(), currResult);
        postorderHelper(node.getRight(), currResult);
        currResult.addLast(node.getData());
    }

}