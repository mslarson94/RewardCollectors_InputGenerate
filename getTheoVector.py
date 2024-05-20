'''
getTheoVector.py
Created on May 13 2024
@author: myra

function to get the closest theorhetical position of either coins or starting positions 
to an actual position produced from Reward Collectors Task output data
'''

import math
from dataConfigs import *

def getTheoVector(p, WhichTheo, droppedY=False):
	'''
	Takes actual coordinates supplied from Magic Leap 2 Reward Collectors Task (RC) output 
	and finds the closest theoretical vector - be it coin position or starting position. 
	
	p = List of Floats. Actual x,y,z coordinates from the HeadPosDelta Column in the RC output file 
	
	WhichTheo = String. Which type of theorhetical coordinates to compare p to? Options = 'startPos' or 'coin'
	
	droppedY = Boolean. Whether the y value has already been dropped in the actual coordinate provided, default value is False
	'''
	p = p if droppedY == True else p[:1] + p[2:]
	validWhichTheo = ['coin', 'startPos']
	try:
		if WhichTheo not in validWhichTheo:
			raise ValueError
	except ValueError:
		print("ERROR: WhichTheo must be either 'coin' or 'startPos', try again")

	else:
		#print('stuff worked')
		if WhichTheo == 'startPos':
			positionPossibilities = AN_positions + PO_positions
			whoStartList = ['AN']*9 + ['PO']*9
		else:
			positionPossibilities = [[item for i, item in enumerate(sublist) if i != 2] for sublist in collectionOrder_List]
			coinValues = [sublist[2] for sublist in collectionOrder_List if len(sublist) > 2]

		distances = [math.dist(p, q) for q in positionPossibilities]
		closestPosition = distances.index(min(distances))
		#print('distances', distances)
		#print('*'*25)
		#print('closestPosition', closestPosition, distances[closestPosition], positionPossibilities[closestPosition])
		coinValue = ''
		actualPosition = positionPossibilities[closestPosition]
		if WhichTheo == 'coin':
			coinValue = coinValues[closestPosition]
		else:
			whoStart = whoStartList[closestPosition]
		#print(actualPosition, coinValue, whoStart)
		return actualPosition, coinValue, whoStart


# myCoordinate = [0, 0, 1]
# getTheoVector(myCoordinate, 'startPos')
