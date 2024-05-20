import dataConfigs
import math
import networkx as nx
import matplotlib.pyplot as plt
import imageio
import os
import shutil
import heapq
import imageio.v2 as imageio
import pandas as pd
from collections import OrderedDict

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
    print(collectionOrder_List)
    for i, sublist in enumerate(collectionOrder_List):
        coinName = collectionOrder_List_str[i]
        print(coinName)
        sublist.insert(0, coinName)
        print(sublist)
        if sublist[0] == sublist[1]:
            del sublist[0]
        print('after removing non unique items')
        print(sublist)
        coordinate = [sublist[1], sublist[2]]
        print(coordinate)
        coordinate = tuple(coordinate)
        print(coordinate)
        print('gonna cry', sublist)
        del sublist[1]
        print('blahhhhhhh')
        print(sublist[1])

        del sublist[1]
        print('afterremoving stuff ')
        print(sublist)
        sublist.insert(1, coordinate)
        weight = 0.0
        sublist.insert(3, weight)
        print(sublist)
        key, coordinates, reward, weight = sublist
        result_dict[key] = {'coord':coordinates,'weight': weight}
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

    # # Print all edges with their attributes
    # for u, v, attr in G.edges(data=True):
    #     print(f"Edge from {u} to {v} has weight {attr['weight']}")
    return G

def iterate_startPos():
    startPosList = dataConfigs.AN_positions
    pos_strList = dataConfigs.pos_strList

    del startPosList[-1]
    del pos_strList[-1]
    edgeWeightList =[]
    #print(pos_strList)
    points = getPoints()
    print(points)
    for pos in range(len(pos_strList)):
        posStr = pos_strList[pos]
        coordUnformat = startPosList[pos]
        coord = tuple(coordUnformat)
        print(coord)
        points[posStr] = {'coord':coord, 'reward': 0.0, 'weight': 20.0}
        print(points)
        edgeWeights = getEdgeWeights(points)
        #print(edgeWeights)
        edgeWeightList.append([posStr, edgeWeights])
        del points[posStr]
    return edgeWeightList

graph_startList = iterate_startPos()
print(graph_startList)
# points = getPoints()
# print(points)
# edgeWeights = getEdgeWeights(points)
# print('*'*27)
# print(edgeWeights)

def draw_graph(G, node_colors, edge_colors, pos, frame_id):
    plt.figure(figsize=(8, 6))
    nx.draw(G, pos, node_color=node_colors, edge_color=edge_colors, with_labels=True, node_size=800, font_size=16)
    plt.savefig(f"frames/frame_{frame_id:03d}.png")
    plt.close()

def animate_dijkstra(graph, start_node, filename):
    os.makedirs("frames", exist_ok=True)
    frame_id = 0
    
    pos = nx.spring_layout(graph, seed=42)
    visited = {node: False for node in graph.nodes}
    distances = {node: float("inf") for node in graph.nodes}
    distances[start_node] = 0
    pq = [(0, start_node)]
    node_visitOrder = []
    while pq:
        current_distance, current_node = heapq.heappop(pq)
        node_visitOrder.append(current_node)
        if visited[current_node]:
            continue

        visited[current_node] = True

        # Draw the graph at this step
        node_colors = ["green" if node == current_node else ("red" if visited[node] else "gray") for node in graph.nodes]
        edge_colors = ["black" for edge in graph.edges]
        draw_graph(graph, node_colors, edge_colors, pos, frame_id)
        frame_id += 1

        for neighbor, edge_weight in graph[current_node].items():
            new_distance = current_distance + edge_weight["weight"]
            if not visited[neighbor] and new_distance < distances[neighbor]:
                distances[neighbor] = new_distance
                heapq.heappush(pq, (new_distance, neighbor))
    # Generate the animated GIF
    images = []
    for i in range(frame_id):
        images.append(imageio.imread(f"frames/frame_{i:03d}.png"))
    imageio.mimsave(filename, images, duration=1)

    # Clean up the frames folder
    shutil.rmtree("frames")
    return node_visitOrder

allNodeVisitOrders = []
for specGraph in graph_startList:
    filename = specGraph[0] + "_dijkstra.gif"
    nodevisit = animate_dijkstra(specGraph[1], specGraph[0], filename)
    allNodeVisitOrders.append(nodevisit)

# # Extract column names and transpose data
# column_names = [col[0] for col in allNodeVisitOrders]  # Extract the first item of each sublist as column names
# transposed_data = [col[1:] for col in allNodeVisitOrders]  # Extract the rest of the items for data

# # Create the DataFrame using a dictionary comprehension to align data under each column name
# df = pd.DataFrame({name: data for name, data in zip(column_names, transposed_data)})

# print(df)
# allNodeVisitOrders.insert(0, [dataConfigs.whichCoinSet]*8)
# allNodeVisitOrders.insert(0, ["PureWeightedDijkstra"]*8)
# order_str = [str(i) for i in range(1, 7)]
# column_names = ["TheoPathType", "CoinSet"]
# column_names.extend(order_str)
# allNodeVisitOrders.insert(0, column_names)
# column_names = [item[0] for item in allNodeVisitOrders]
# column_names.insert(0, "CoinSet")
# column_names.insert(0, "TheoPathType")
# print(column_names)

# row_data = [sublist[1:] for sublist in allNodeVisitOrders if len(sublist) > 0]
# row_data.insert(0, [dataConfigs.whichCoinSet]*8)
# row_data.insert(0, ["PureWeightedDijkstra"]*8)
# print(row_data)
# df = pd.DataFrame(columns=column_names, data=row_data)
df = pd.DataFrame(allNodeVisitOrders)

df.insert(0, "CoinSet", [dataConfigs.whichCoinSet]*8)
df.insert(0, 'TheoPathType', ["PureUnweightedDijkstra"]*8)
df.rename(columns={0: "startPosition"}, inplace=True)
df.to_csv("WeightedDijkstra.csv", index=False)

# from IPython.display import Image
# Image(filename="dijkstra.gif")