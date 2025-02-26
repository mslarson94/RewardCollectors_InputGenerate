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



def calculate_dynamic_edge_weight(coord1, coord2, weight1, weight2, unvisited_coins):
    """
    Calculate the dynamic weight of an edge based on:
    - Euclidean distance
    - Node weights
    - Remaining valuable coins
    """
    distance = math.sqrt((coord1[0] - coord2[0])**2 + (coord1[1] - coord2[1])**2)
    average_weight = (weight1 + weight2) / 2

    # Adjust weight based on unvisited coins (only those still in the graph)
    bonus = sum(CoinSet[coin]['pts'] for coin in unvisited_coins if coin in CoinSet)

    # Ensure positive weights
    weight = max(distance + average_weight + bonus / 2, 0.01)  # Avoid 0.00 weights
    return weight



def getEdgeWeights(points):
    """
    Generate a graph with nodes as coin locations and edges weighted dynamically.
    """
    G = nx.Graph()
    
    # Ensure `PPE` and `NPE` are removed before adding nodes
    points = {k: v for k, v in points.items() if k not in ["PPE", "NPE"]}

    # Add nodes
    for point, attrs in points.items():
        G.add_node(point, coords=attrs['coords'], weight=attrs['weight'])

    # Add edges with calculated weights
    nodes = list(G.nodes(data=True))  # List of tuples (node, attr_dict)
    for i in range(len(nodes)):
        for j in range(i + 1, len(nodes)):
            node1, attrs1 = nodes[i]
            node2, attrs2 = nodes[j]

            edge_weight = calculate_dynamic_edge_weight(
                attrs1['coords'], attrs2['coords'],
                attrs1['weight'], attrs2['weight'], 
                list(points.keys())  # Ensure only valid coins are passed
            )

            G.add_edge(node1, node2, weight=edge_weight)

    return G


def update_graph_after_visit(graph, visited_node, unvisited_coins):
    """
    Update the graph after visiting a node:
    - Keep the node in the graph but remove outgoing edges
    - Recalculate edge weights for remaining nodes
    """
    if visited_node in unvisited_coins:
        unvisited_coins.remove(visited_node)

    # Instead of removing the node, remove its edges
    graph.remove_edges_from(list(graph.edges(visited_node)))

    # Recalculate edge weights based on new graph state
    for u, v, attrs in list(graph.edges(data=True)):
        coord1, weight1 = graph.nodes[u]['coords'], graph.nodes[u]['weight']
        coord2, weight2 = graph.nodes[v]['coords'], graph.nodes[v]['weight']
        
        attrs['weight'] = calculate_dynamic_edge_weight(coord1, coord2, weight1, weight2, unvisited_coins)
    
    return graph



def dynamic_traversal_with_start(G, start_position):
    """
    Perform dynamic traversal from any starting position to collect all valuable coins in order.
    """
    unvisited_coins = {node for node in G.nodes if node not in ["PPE", "NPE"]}  # Remove PPE & NPE
    total_score = 0
    collected_coins = []
    full_path = []

    pq = []  # Priority queue (min-heap)
    heapq.heappush(pq, (0, start_position))  # Start from the actual start node

    while pq and unvisited_coins:
        current_weight, current_node = heapq.heappop(pq)

        # Skip already visited nodes
        if current_node not in unvisited_coins:
            continue

        # Collect coin points (but only if it's a coin, not a start position)
        if current_node in CoinSet:
            multiplier = 2 if len(collected_coins) < 2 else 1
            total_score += CoinSet[current_node]['pts'] * multiplier
            collected_coins.append(current_node)

        # Append to path, but prevent duplicates
        if not full_path or full_path[-1] != current_node:
            full_path.append(current_node)

        # Mark node as visited
        unvisited_coins.remove(current_node)

        # Push unvisited neighbors into the queue
        for neighbor in G.neighbors(current_node):
            if neighbor in unvisited_coins:
                heapq.heappush(pq, (G[current_node][neighbor]['weight'], neighbor))

    return full_path, total_score





def iterate_startPos():
    startPosList = AN_positions.copy()  # Make sure we don't modify the original list

    del startPosList[-1]
    del pos_strList[-1]
    edgeWeightList = []

    points = CoinSet.copy()  # Ensure we don't modify the original CoinSet
    points = {k: v for k, v in points.items() if k not in ["PPE", "NPE"]}  # Remove PPE and NPE

    for pos in range(len(pos_strList)):
        posStr = pos_strList[pos]
        coord = startPosList[pos]

        # Add start position to the points dictionary
        points[posStr] = {'coords': coord, 'pts': 0.0, 'weight': 0.0}

        # Generate graph with edges including the new start position
        edgeWeights = getEdgeWeights(points)

        # Ensure start position is connected to at least one coin
        for coin in ["HV", "LV", "NV"]:
            if coin in points:
                edgeWeights.add_edge(posStr, coin, weight=calculate_dynamic_edge_weight(
                    points[posStr]['coords'], points[coin]['coords'], 
                    points[posStr]['weight'], points[coin]['weight'], list(points.keys())
                ))

        edgeWeightList.append([posStr, edgeWeights])
        del points[posStr]  # Remove the start position for the next iteration

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

# Ensure PPE and NPE are removed before generating the graph
cleaned_CoinSet = {k: v for k, v in CoinSet.items() if k not in ["PPE", "NPE"]}
G = getEdgeWeights(cleaned_CoinSet)  # Use cleaned version

# Print Edge Weights
for u, v, attrs in G.edges(data=True):
    print(f"Edge ({u} -> {v}) weight: {attrs['weight']:.2f}")

# Ensure start graph does not include PPE or NPE before traversal
start_graph = graph_startList[0][1]
start_graph = start_graph.subgraph([node for node in start_graph.nodes if node not in ["PPE", "NPE"]])

start_node = graph_startList[0][0]  # Start from first position
path, score = dynamic_traversal_with_start(start_graph, start_node)

print("Optimal Path:", path)
print("Total Score:", score)

# Ensure start positions are included in the graph before plotting
filtered_graph = G.subgraph([node for node in G.nodes if node not in ["PPE", "NPE"]]).copy()  # Copy to make it mutable

# Add an edge from the start position to the first collected coin
if len(path) > 1:  # Ensure at least one coin is collected
    first_coin = path[1]  # First collected coin
    filtered_graph.add_edge(start_node, first_coin, weight=1.0)  # Small weight for visualization

# Include start positions in the node dictionary
filtered_nodes = {node: cleaned_CoinSet[node]["coords"] for node in cleaned_CoinSet}

# Add start position if it's not already there
if start_node not in filtered_nodes:
    filtered_nodes[start_node] = graph_startList[0][1].nodes[start_node]["coords"]

# Create color mapping: Start position = Blue, Coins = Red
node_colors = ["blue" if node == start_node else "red" for node in filtered_graph.nodes]

nx.draw(filtered_graph, filtered_nodes, with_labels=True, node_size=800, node_color=node_colors, font_size=10)
plt.show()



# Ensure DataFrame is structured properly
df = pd.DataFrame(allNodeVisitOrders)

if not df.empty:
    df.insert(0, "CoinSet", [whichCoinSet] * len(df))
    df.insert(0, "TheoPathType", ["PureWeightedDijkstra"] * len(df))
    df.rename(columns={0: "startPosition"}, inplace=True)
    df.to_csv("WeightedDijkstra.csv", index=False)
else:
    print("⚠ Warning: Dataframe is empty, skipping CSV export.")
