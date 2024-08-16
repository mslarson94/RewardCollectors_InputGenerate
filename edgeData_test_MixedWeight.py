import dataConfigs_TheoPaths
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
from helper_functions.path_check import path_check

def getPoints_Mixed(whichMixedStart):
    '''
     import the coin set data & format in a points dict like this 
     points = {
                'A': {'coord': (1, 2), 'weight': 1.5},
                'B': {'coord': (4, 6), 'weight': 2.5},
                'C': {'coord': (7, 8), 'weight': 3.0}
                }

    whichMixedStart : either 'Post' or 'Pre'
    '''
    result_dict = {}
    collectionOrder_List = dataConfigs_TheoPaths.collectionOrder_List
    collectionOrder_List_str = dataConfigs_TheoPaths.collectionOrder_List_str
    #print(collectionOrder_List)
    for i, sublist in enumerate(collectionOrder_List):
        coinName = collectionOrder_List_str[i]
        #print(coinName)
        sublist.insert(0, coinName)
        #print(sublist)
        if sublist[0] == sublist[1]:
            del sublist[0]
        #print('after removing non unique items')
        #print(sublist)
        coordinate = [sublist[1], sublist[2]]
        #print(coordinate)
        coordinate = tuple(coordinate)
        #print(coordinate)
        #print('gonna cry', sublist)
        del sublist[1]
        #print('blahhhhhhh')
        #print(sublist[1])

        del sublist[1]
        #print('afterremoving stuff ')
        #print(sublist)
        sublist.insert(1, coordinate)
        weight = ""
        if whichMixedStart == "Post": 
            if sublist[0].startswith('N'):
                weight = 0.0
            elif sublist[0].startswith('L'):
                weight = 0.0

        elif whichMixedStart == "Pre":
            elif sublist[0].startswith('H'):
                weight = 0.0


        sublist.insert(3, weight)
        #print(sublist)
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
    return G

def iterate_startPos(whichMixedStart):
    points = getPoints_Mixed(whichMixedStart)
    startPosList = []
    pos_strList = []
    edgeWeightList =[]

    if whichMixedStart == 'Pre':
        startPosList = dataConfigs_TheoPaths.AN_positions
        pos_strList = dataConfigs_TheoPaths.pos_strList
        del startPosList[-1]
        del pos_strList[-1]

    elif whichMixedStart == 'Post':
        startPosList = [dataConfigs_TheoPaths.HV_1, dataConfigs_TheoPaths.HV_2]
        pos_strList = ['HV_1', 'HV_2']

    for pos in range(len(pos_strList)):
        posStr = pos_strList[pos]
        coordUnformat = startPosList[pos]
        coord = tuple(coordUnformat)
        #print(coord)
        points[posStr] = {'coord':coord, 'reward': 0.0, 'weight': 20.0}
        #print(points)
        edgeWeights = getEdgeWeights(points)
        #print(edgeWeights)
        edgeWeightList.append([posStr, edgeWeights])
        del points[posStr]
    return edgeWeightList


def dijkstra_noAnimate(graph, start_node, filename):
    
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

        for neighbor, edge_weight in graph[current_node].items():
            new_distance = current_distance + edge_weight["weight"]
            if not visited[neighbor] and new_distance < distances[neighbor]:
                distances[neighbor] = new_distance
                heapq.heappush(pq, (new_distance, neighbor))

    return node_visitOrder
