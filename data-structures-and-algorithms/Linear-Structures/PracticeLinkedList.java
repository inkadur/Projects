// Java program to implement 
// a Singly Linked List 
public class PracticeLinkedList { 

	Node head; // head of list 

	// Linked list Node. 
	// This inner class is made static 
	// so that main() can access it 
	static class Node { 

		int data; 
		Node next; 

		// Constructor 
		Node(int d) 
		{ 
			data = d; 
			next = null; 
		} 
	} 

	// Method to insert a new node at end of list
	public static LinkedList insert(LinkedList list, int data) 
	{ 
		// Create a new node with given data 
		Node new_node = new Node(data); 
		

		// If the Linked List is empty, 
		// then make the new node as head 
		if (list.head == null) { 
			list.head = new_node; 
		} 
		else { 
			// Else traverse till the last node 
			// and insert the new_node there 
			Node last = list.head; 
			while (last.next != null) { 
				last = last.next; 
			} 

			// Insert the new_node at last node 
			last.next = new_node; 
		} 

		// Return the list by head 
		return list; 
	} 

	// Method to print the LinkedList. 
	public static void printList(LinkedList list) 
	{ 
		Node currNode = list.head; 
	
		System.out.print("LinkedList: "); 
	
		// Traverse through the LinkedList 
		while (currNode != null) { 
			// Print the data at current node 
			System.out.print(currNode.data + " "); 
	
			// Go to next node 
			currNode = currNode.next; 
		} 
	} 

	// Method to remove any duplicates recursively
	public static LinkedList removeDuplicates(Node currNode, LinkedList finalList){
		// edge case: the list is empty
		if (currNode == null){
			return null;
		}
		// add data to finalList in all cases
		finalList = insert(finalList, currNode.data);
		// base case: no next node
		if (currNode.next == null){
			return finalList;
		}
		// base case: final two nodes are matching
		else if (currNode.next.next == null && currNode.data == currNode.next.data){
			return finalList;
		}
		// recusive cases
		// Case 1: there is a duplicate
		else if (currNode.data == currNode.next.data){
			// look through all duplicates before recursively calling function
			int data = currNode.data;
			while (currNode != null && currNode.data == data){
				currNode = currNode.next;
			}
			if (currNode == null){
				return finalList;
			}
			else{
				return removeDuplicates(currNode, finalList);
			}
		}
		// Case 2: there is no duplicate, call function on next node
		else {
			return removeDuplicates(currNode.next, finalList);
		}
		
	}
	
	// Driver code 
    public static void main(String[] args) {
		System.out.println("Test on Empty List");
		LinkedList list = new LinkedList();
		//printList(list);
		LinkedList newList = removeDuplicates(list.head, new LinkedList());
		//printList(newList);

		System.out.println("Test on List with 1 element");
		LinkedList list1 = new LinkedList();
		list1 = insert(list1, 0);
		printList(list1);
		LinkedList newList1 = removeDuplicates(list1.head,  new LinkedList());
		printList(newList1);
		System.out.println();

		System.out.println("Test on List with no duplicates");
		LinkedList list2 = new LinkedList();
		list2 = insert(list2, 1);
		list2 = insert(list2, 2);
		list2 = insert(list2, 3);
		printList(list2);
		LinkedList newList2 = removeDuplicates(list2.head,  new LinkedList());
		printList(newList2);
		System.out.println();

		System.out.println("Test on List with all same value (2)");
		LinkedList list3 = new LinkedList();
		list3 = insert(list3, 1);
		list3 = insert(list3, 1);
		printList(list3);
		LinkedList newList3 = removeDuplicates(list3.head,  new LinkedList());
		printList(newList3);
		System.out.println();

		System.out.println("Test on List with all same value");
		LinkedList list4 = new LinkedList();
		list4 = insert(list4, 1);
		list4 = insert(list4, 1);
		list4 = insert(list4, 1);
		list4 = insert(list4, 1);
		list4 = insert(list4, 1);
		printList(list4);
		LinkedList newList4 = removeDuplicates(list4.head,  new LinkedList());
		printList(newList4);
		System.out.println();

		System.out.println("Test on List with multiple different duplicates");
		LinkedList list5 = new LinkedList();
		list5 = insert(list5, 0);
		list5 = insert(list5, 0);
		list5 = insert(list5, 1);
		list5 = insert(list5, 1);
		list5 = insert(list5, 2);
		list5 = insert(list5, 2);
		printList(list5);
		LinkedList newList5 = removeDuplicates(list5.head,  new LinkedList());
		printList(newList5);
		System.out.println();

		System.out.println("Test on List with mix of duplicates and non");
		LinkedList list6 = new LinkedList();
		list6 = insert(list6, 0);
		list6 = insert(list6, 0);
		list6 = insert(list6, 1);
		list6 = insert(list6, 2);
		list6 = insert(list6, 2);
		list6 = insert(list6, 2);
		list6 = insert(list6, 3);
		printList(list6);
		LinkedList newList6 = removeDuplicates(list6.head,  new LinkedList());
		printList(newList6);

        
    }
	
}
