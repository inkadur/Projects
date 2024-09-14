import java.util.List;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.*;
public class TraversalTest {
    public TreeNode<Integer> createTestTree(){
        TreeNode<Integer> root = new TreeNode<>(0);
        root.setLeft(new TreeNode<>(-2));
        root.setRight(new TreeNode<>(2));
        root.getLeft().setLeft(new TreeNode<>(-3));
        root.getLeft().setRight(new TreeNode<>(-1));
        root.getRight().setLeft(new TreeNode<>(1));
        root.getRight().setRight(new TreeNode<>(3));
        return root;
    }

    @Test
    public void testPreorderTraversal(){
        TreeNode<Integer> root = createTestTree();
        Traversals<Integer> traversal = new Traversals<>();
        List<Integer> result = traversal.preorder(root);
        List<Integer> expected = Arrays.asList(0, -2, -3, -1, 2, 1, 3);
        assertEquals(expected, result);
    }

    @Test
    public void testPreorderEmpty(){
        Traversals<Integer> traversal = new Traversals<>();
        TreeNode<Integer> root = null;
        List<Integer> result = traversal.preorder(root);
        List<Integer> expected = Arrays.asList();
        assertEquals(expected, result);
    }

    @Test
    public void testPreorderSingle(){
        TreeNode<Integer> root = new TreeNode<>(0);
        Traversals<Integer> traversal = new Traversals<>();
        List<Integer> result = traversal.preorder(root);
        List<Integer> expected = Arrays.asList(0);
        assertEquals(expected, result);
    }

    @Test
    public void testInorderTraversal(){
        TreeNode<Integer> root = createTestTree();
        Traversals<Integer> traversal = new Traversals<>();
        List<Integer> result = traversal.inorder(root);
        List<Integer> expected = Arrays.asList(-3, -2, -1, 0, 1, 2,3);
        assertEquals(expected, result);  

    }

    @Test
    public void testPostorderTraversal(){
        TreeNode<Integer> root = createTestTree();
        Traversals<Integer> traversal = new Traversals<>();
        List<Integer> result = traversal.postorder(root);
        List<Integer> expected = Arrays.asList(-3, -1, -2, 1, 3, 2, 0);
        assertEquals(expected, result);  

    }

    public TreeNode<Integer> newTestTree(){
        TreeNode<Integer> root = new TreeNode<>(5);
        root.setLeft(new TreeNode<>(4));
        root.setRight(new TreeNode<>(7));
        root.getLeft().setLeft(new TreeNode<>(1));
        root.getLeft().getLeft().setLeft(new TreeNode<>(0));
        root.getLeft().getLeft().setRight(new TreeNode<>(2));
        root.getRight().setRight(new TreeNode<>(9));
        root.getRight().getRight().setLeft(new TreeNode<>(8));
        return root;
    }

    @Test
    public void testFailedInorderTraversal(){
        TreeNode<Integer> root = newTestTree();
        Traversals<Integer> traversal = new Traversals<>();
        List<Integer> result = traversal.inorder(root);
        List<Integer> expected = Arrays.asList(0, 1, 2, 4, 5, 7 ,8, 9);
        assertEquals(expected, result);  

    }

    @Test
    public void testFailedPostorderTraversal(){
        TreeNode<Integer> root = newTestTree();
        Traversals<Integer> traversal = new Traversals<>();
        List<Integer> result = traversal.postorder(root);
        List<Integer> expected = Arrays.asList(0, 2, 1, 4, 8 ,9, 7 ,5);
        assertEquals(expected, result);  

    }
    
    
}
