import dataConfigs_TheoPaths
import edgeData_test
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

graph_startList = edgeData_test.iterate_startPos(dataConfigs_TheoPaths.whichWeight)
#print(graph_startList)
allNodeVisitOrders = []

for specGraph in graph_startList:
	print('specGraph[0]', specGraph[0])
	filename = dataConfigs_TheoPaths.outPath + '/' + specGraph[0] + "_dijkstra.gif"
	nodevisit =edgeData_test.animate_dijkstra(specGraph[1], specGraph[0], filename)
	allNodeVisitOrders.append(nodevisit)

df = pd.DataFrame(allNodeVisitOrders)

df.insert(0, "CoinSet", [dataConfigs_TheoPaths.whichCoinSet]*8)
df.insert(0, 'TheoPathType', [dataConfigs_TheoPaths.whichWeight]*8)
df.rename(columns={0: "startPosition"}, inplace=True)

outFile = dataConfigs_TheoPaths.outPath + '.csv'
df.to_csv(outFile, index=False)