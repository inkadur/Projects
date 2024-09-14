import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import javax.management.openmbean.ArrayType;

import java.util.LinkedList;

public class BSTTraversals <T extends Comparable<? super T>> {
    public List<T> preOrder(BSTNode<T> root){
        ArrayList<T> result = new ArrayList<>();
        rPreOrder(root, result);
        return result;
    }

    private void rPreOrder(BSTNode<T> node, ArrayList<T> result){
        result.addLast(node.getData());
        rPreOrder(node.getLeft(), result);
        rPreOrder(node.getRight(), result);
    }

    public List<T> levelOrder(BSTNode<T> root){
        ArrayList<T> result = new ArrayList<T>();
        Queue<BSTNode<T>> q = new LinkedList<BSTNode<T>>();
        q.add(root);
        while (q.peek() != null){
            BSTNode<T> node = q.remove();
            if (node.getLeft() != null){
                q.add(node.getLeft());
            }
            if (node.getRight() != null){
                q.add(node.getRight());
            }
            result.addLast(node.getData());
        }
        return result;  
    }
}
