import dataConfigs
import math
import networkx as nx


def getPoints():
    '''
     import the coin set data & format in a points dict like this 
     points = {
                'A': {'coord': (1, 2), 'weight': 1.5},
                'B': {'coord': (4, 6), 'weight': 2.5},
                'C': {'coord': (7, 8), 'weight': 3.0}
                }
    '''
    result_dict = {}
    collectionOrder_List = dataConfigs.collectionOrder_List
    collectionOrder_List_str = dataConfigs.collectionOrder_List_str
    for i, sublist in enumerate(collectionOrder_List):
        coinName = collectionOrder_List_str[i]
        sublist.insert(0, coinName)
        #print(sublist)
        coordinate = [sublist[1], sublist[2]]
        coordinate = tuple(coordinate)
        #print(coordinate)
        del sublist[1]
        del sublist[1]
        #print(sublist)
        sublist.insert(1, coordinate)
        #print(sublist)
        key, coordinates, weight = sublist
        result_dict[key] = {'coord':coordinates, 'weight': weight}
        #print(result_dict[key])
    #print(result_dict)
    startPos = dataConfigs.actual_startPos
    key, start_coordinate, weight = ['startPos', startPos, 0.0]
    result_dict[key] = {'coord':start_coordinate, 'weight': 0.0}
    return result_dict

def calculate_edge_weight(coord1, coord2, weight1, weight2):
    # Calculate Euclidean distance between points
    distance = math.sqrt((coord1[0] - coord2[0])**2 + (coord1[1] - coord2[1])**2)
    # Average the weights
    average_weight = (weight1 + weight2) / 2
    # Combine the distance and average weight
    return distance + average_weight

def getEdgeWeights(points):
    # Create a graph
    G = nx.Graph()

    # Add nodes
    for point, attrs in points.items():
        G.add_node(point, coord=attrs['coord'], weight=attrs['weight'])

    # Add edges with calculated weights
    nodes = list(G.nodes(data=True))  # List of tuples (node, attr_dict)
    for i in range(len(nodes)):
        for j in range(i + 1, len(nodes)):
            node1, attrs1 = nodes[i]
            node2, attrs2 = nodes[j]
            edge_weight = calculate_edge_weight(attrs1['coord'], attrs2['coord'], attrs1['weight'], attrs2['weight'])
            G.add_edge(node1, node2, weight=edge_weight)

    # Print all edges with their attributes
    for u, v, attr in G.edges(data=True):
        print(f"Edge from {u} to {v} has weight {attr['weight']}")
    return G

points = getPoints()
print(points)
edgeWeights = getEdgeWeights(points)
print('*'*27)
print(edgeWeights)