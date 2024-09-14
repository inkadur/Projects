
from collections import defaultdict
import heapq as heap


###########################################
class Vertex():
  def __init__ (self, label):
    self.label = label
    self.visited = False

  # determine if a vertex was visited
  def was_visited(self):
    return self.visited

  # determine the label of the vertex
  def get_label(self):
    return self.label

  # string representation of the vertex
  def __str__(self):
    return str(self.label)

###########################################
class Graph():
  
  def __init__(self):
    self.vertices = []
    self.adjMat = []

  def has_vertex(self, label):
    '''
    Checks if a vertex is already in the graph
    '''
    for i in range(len(self.vertices)):
        if (label == (self.vertices[i]).get_label()):
          return True
    return False
  
  def get_index(self, label):
    '''
    Given the label get the index of a vertex
    '''
    for i in range(len(self.vertices)):
      if (label == (self.vertices[i]).get_label()):
        return i
    return -1

  
  def add_vertex(self, label):
    '''
    Add a Vertex with a given label to the graph
    '''
    
    if (self.has_vertex(label)):
      return

    # add vertex to the list of vertices
    self.vertices.append(Vertex(label))

    # add a new column in the adjacency matrix
    for i in range(len(self.vertices) - 1):
      (self.adjMat[i]).append(0)

    # add a new row for the new vertex
    new_row = []
    for i in range(len(self.vertices)):
      new_row.append(0)
    self.adjMat.append(new_row)

  
  def add_directed_edge(self, start, finish, weight = 1):
    '''
    Adds a weighted directed edge to graph. 
    '''
    self.adjMat[start][finish] = weight

  
  def add_undirected_edge(self, start, finish, weight = 1):
    '''
    Adds weighted undirected edge to graph. 
    '''
    self.adjMat[start][finish] = weight
    self.adjMat[finish][start] = weight

  def __str__(self):
    '''
    A simple string representation of the graph in Adjancy Matrix. 
    '''
    tmp = "\nVerticies are: \n"
    for vertex in self.vertices:
      tmp += str(vertex) + str("\n")

    tmp += "Adjancy Matrix is: \n"
    for i in range(len(self.adjMat)):
      tmp += "\n"
      tmp += str(self.adjMat[i])

    tmp += "\n"
    return tmp



def dijkstra(graph, start_node):
    '''
    This Function implements the dijkstra algorithm 
    '''
    parent_map = {} # A list to store parent of each node

    # Initial all node distances to infinity
    node_distances = defaultdict(lambda: float('inf'))
    node_distances[start_node] = 0

    pq = []
    heap.heappush(pq, (0, start_node))
    visited = set()

    while pq:
      _, node = heap.heappop(pq)
      visited.add(node)

      for adj_node, weight in enumerate(graph.adjMat[node]):
        if adj_node in visited:
          continue
        if weight == 0:
          continue
        new_distance = node_distances[node] + weight

        if node_distances[adj_node] > new_distance:
          parent_map[adj_node] = node
          node_distances[adj_node] = new_distance
          heap.heappush(pq, (new_distance, adj_node))
    return parent_map, node_distances


def bellmann_ford(graph, start_node):
    '''
    This method implements the bellmann ford algorithm 
    '''

    parent_map = {}  # A list to store parent of each node

    # Initial all node distances to infinity
    node_distances = defaultdict(lambda: float('inf'))
    node_distances[start_node] = 0

    for node in graph.adjMat:
      for adj_node, distance in enumerate(node):
        temp_distance = node_distances[graph.adjMat.index(node)] + distance
        if temp_distance < node_distances[adj_node] and temp_distance > 0:
          node_distances[adj_node] = temp_distance
          parent_map[adj_node] = graph.adjMat.index(node)



    return parent_map, node_distances






def main():
  graph = Graph()

  graph.add_vertex(0)
  graph.add_vertex(1)
  graph.add_vertex(2)
  graph.add_vertex(3)
  graph.add_vertex(4)


  # Add Edges 
  # From Vertex 0 
  graph.add_directed_edge(0, 1, 11)
  graph.add_directed_edge(0, 2, 5)

  # From Node 1 
  graph.add_directed_edge(1, 3, 2)
  
  # From Node 3
  graph.add_directed_edge(2, 1, 4)
  graph.add_directed_edge(2, 3, 6)

  print(graph)


  
  parent_map, node_distances = dijkstra(graph, 0)
  print("Dijkstra Distances", dict(node_distances))
  print("Dijkstra Parent Map", parent_map)


  parent_map, node_distances = bellmann_ford(graph, 0)
  print("Bellman-Ford Distances", dict(node_distances))
  print("Bellman-Ford Parent Map", parent_map)



if __name__ == "__main__":
  main()



