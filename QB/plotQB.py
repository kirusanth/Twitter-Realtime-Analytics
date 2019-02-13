# libraries
import numpy as np
import matplotlib.pyplot as plt
 
# set width of bar
barWidth = 0.17
 


#This will add pos, neg, and neu values to list which it gets from the line it splits
def splitter (line):
	output=list()
	splited_line = line.split()
	var = splited_line[0]
	output.append (float(splited_line[1]))
	output.append (float(splited_line[2]))
	output.append (float(splited_line[3]))
	return (var,output)

# initalize the values to allow realtime dynamic ploting
var1,bars1 = 0,0
var2,bars2 = 0,0
var3,bars3 = 0,0
var4,bars4 = 0,0
var5,bars5 = 0,0

with open("QB.txt","r") as file:
	data = file.readlines()
	#avoid boundry exceptions if the line is not present
	if (len(data) >= 1):
		var1,bars1 = splitter (data[0])
	if (len(data) >= 2):
		var2,bars2 = splitter (data[1])
	if (len(data) >= 3):
		var3,bars3 = splitter (data[2])
	if (len(data) >= 4):
		var4,bars4 = splitter (data[3])
	if (len(data) >= 5):
		var5,bars5 = splitter (data[4])
	

 
# Set position of bar on X axis
r1 = np.arange(len(bars1))
r2 = [x + barWidth for x in r1]
r3 = [x + barWidth for x in r2]
r4 = [x + barWidth for x in r3]
r5 = [x + barWidth for x in r4]


 
# Make the plot (if statements for dynamic changes occur when the lines are not present)
if var1!=0 and bars1 !=0 :
	plt.bar(r1, bars1, color='r', width=barWidth, edgecolor='white', label=var1)
if var2!=0 and bars2 !=0 :
	plt.bar(r2, bars2, color='g', width=barWidth, edgecolor='white', label=var2)
if var3!=0 and bars3 !=0 :
	plt.bar(r3, bars3, color='b', width=barWidth, edgecolor='white', label=var3)
if var4!=0 and bars4 !=0 :
	plt.bar(r4, bars4, color='y', width=barWidth, edgecolor='white', label=var4)
if var5!=0 and bars5 !=0 :
	plt.bar(r5, bars5, color='m', width=barWidth, edgecolor='white', label=var5)
 
# Add xticks on the middle of the group bars
plt.xlabel('group', fontweight='bold')
plt.ylabel('Average Scores', fontweight='bold')
plt.xticks([r + barWidth for r in range(len(bars1))], ['Negative', 'Positive', 'Neutral'])
 
# Create legend & Show graphic
plt.legend()
plt.show()
plt.savefig("output")
