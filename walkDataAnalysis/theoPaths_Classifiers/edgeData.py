from RC_utilities.configs.dataConfigs_3Coins import * # Import from the package
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



def calculate_dynamic_edge_weight(coord1, coord2, weight1, weight2, unvisited_coins=[]):
    """
    Calculate the dynamic weight of an edge based on the Euclidean distance and coin values.
    """
    distance = math.sqrt((coord1[0] - coord2[0])**2 + (coord1[1] - coord2[1])**2)
    average_weight = (weight1 + weight2) / 2

    # Adjust weight based on the value of unvisited coins
    bonus = sum(COIN_POINTS[coin] for coin in unvisited_coins)
    return distance + average_weight - bonus  # Prioritize paths to valuable coins

def getEdgeWeights(points):
    # Create a graph
    G = nx.Graph()

    # Add nodes
    for point, attrs in points.items():
        G.add_node(point, coords=attrs['coords'], weight=attrs['weight'])

    # Add edges with calculated weights
    nodes = list(G.nodes(data=True))  # List of tuples (node, attr_dict)
    for i in range(len(nodes)):
        for j in range(i + 1, len(nodes)):
            node1, attrs1 = nodes[i]
            node2, attrs2 = nodes[j]
            edge_weight = calculate_dynamic_edge_weight(attrs1['coords'], attrs2['coords'], attrs1['weight'], attrs2['weight'])
            G.add_edge(node1, node2, weight=edge_weight)

    return G

def update_graph_after_visit(graph, visited_node, unvisited_coins):
    """
    Remove the visited node and recalculate edge weights for the remaining graph.
    """
    unvisited_coins.remove(visited_node)
    graph.remove_node(visited_node)

    # Recalculate edge weights for remaining nodes
    for u, v, attrs in graph.edges(data=True):
        coord1 = graph.nodes[u]['coords']
        coord2 = graph.nodes[v]['coords']
        weight1 = graph.nodes[u]['weight']
        weight2 = graph.nodes[v]['weight']
        attrs['weight'] = calculate_dynamic_edge_weight(coord1, coord2, weight1, weight2, unvisited_coins)

    return graph


def dynamic_traversal_with_start(points, start_position):
    """
    Perform dynamic traversal, starting from a given position, collecting coins, and recalculating edge weights.
    """
    # Create the initial graph
    G = nx.Graph()
    for point, attrs in points.items():
        G.add_node(point, coords=attrs['coords'], weight=attrs['weight'])

    for point1 in points:
        for point2 in points:
            if point1 != point2:
                coord1 = points[point1]['coords']
                coord2 = points[point2]['coords']
                weight1 = points[point1]['weight']
                weight2 = points[point2]['weight']
                edge_weight = calculate_dynamic_edge_weight(coord1, coord2, weight1, weight2, points.keys())
                G.add_edge(point1, point2, weight=edge_weight)

    # Perform traversal
    collected_coins = []
    unvisited_coins = list(points.keys())
    total_score = 0

    # Start traversal from the given starting position
    current_node = start_position

    while unvisited_coins:
        # Find the next node to visit (min weight edge)
        neighbors = [(neighbor, G[current_node][neighbor]["weight"]) for neighbor in G.neighbors(current_node)]
        neighbors.sort(key=lambda x: x[1])  # Sort by edge weight
        next_node = neighbors[0][0]  # Choose the nearest node

        # Collect points if the node is a coin
        if next_node in COIN_POINTS:
            multiplier = 2 if len(collected_coins) < 2 else 1
            total_score += COIN_POINTS[next_node] * multiplier
            collected_coins.append(next_node)

        # Update graph and state
        G = update_graph_after_visit(G, next_node, unvisited_coins)
        current_node = next_node  # Move to the next node

    return collected_coins, total_score


def iterate_startPos():
    startPosList = AN_positions

    del startPosList[-1]
    del pos_strList[-1]
    edgeWeightList =[]

    points = CoinSet
    del points['PPE']
    del points['NPE']
    print(points)
    for pos in range(len(pos_strList)):
        posStr = pos_strList[pos]
        coord = startPosList[pos]
        print(coord)
        points[posStr] = {'coords':coord, 'pts': 0.0, 'weight': 0.0}
        print(points)
        edgeWeights = getEdgeWeights(points)
        #print(edgeWeights)
        edgeWeightList.append([posStr, edgeWeights])
        del points[posStr]
    return edgeWeightList

graph_startList = iterate_startPos()
print(graph_startList)

def draw_graph_with_labels(G, node_colors, edge_colors, pos, frame_id):
    """
    Draw the graph with node and edge labels and save as an image.
    """
    plt.figure(figsize=(8, 6))
    
    # Draw nodes with specified colors
    nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=800)
    
    # Draw edges with specified colors
    nx.draw_networkx_edges(G, pos, edge_color=edge_colors, width=2)
    
    # Draw node labels
    nx.draw_networkx_labels(G, pos, font_size=12, font_color="white")
    
    # Add edge labels for weights
    edge_labels = nx.get_edge_attributes(G, "weight")
    formatted_edge_labels = {k: f"{v:.2f}" for k, v in edge_labels.items()}  # Format weights to 2 decimal places
    nx.draw_networkx_edge_labels(G, pos, edge_labels=formatted_edge_labels, font_size=10)
    
    # Save the frame
    plt.savefig(f"frames/frame_{frame_id:03d}.png")
    plt.close()

def animate_dijkstra_with_labels(graph, start_node, filename):
    """
    Animate Dijkstra's algorithm with edge labels annotated in each frame.
    """
    os.makedirs("frames", exist_ok=True)
    frame_id = 0
    
    pos = nx.spring_layout(graph, seed=42)  # Generate graph layout
    visited = {node: False for node in graph.nodes}
    distances = {node: float("inf") for node in graph.nodes}
    distances[start_node] = 0
    pq = [(0, start_node)]
    node_visit_order = []
    
    while pq:
        current_distance, current_node = heapq.heappop(pq)
        node_visit_order.append(current_node)
        if visited[current_node]:
            continue

        visited[current_node] = True

        # Draw the graph at this step with labels
        node_colors = [
            "green" if node == current_node else ("red" if visited[node] else "gray")
            for node in graph.nodes
        ]
        edge_colors = ["black" for edge in graph.edges]
        draw_graph_with_labels(graph, node_colors, edge_colors, pos, frame_id)
        frame_id += 1

        for neighbor, edge_data in graph[current_node].items():
            new_distance = current_distance + edge_data["weight"]
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
    return node_visit_order

allNodeVisitOrders = []
for specGraph in graph_startList:
    filename = specGraph[0] + "_dijkstra.gif"
    nodevisit = animate_dijkstra_with_labels(specGraph[1], specGraph[0], filename)
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

df.insert(0, "CoinSet", [whichCoinSet]*8)
df.insert(0, 'TheoPathType', ["PureWeightedDijkstra"]*8)
df.rename(columns={0: "startPosition"}, inplace=True)
df.to_csv("WeightedDijkstra.csv", index=False)

# from IPython.display import Image
# Image(filename="dijkstra.gif")