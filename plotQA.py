import matplotlib.pyplot as plot; 
import matplotlib.pyplot as plot
import seaborn as sns
import numpy as np

i = 0
hashtags = ()
counts = ()
# open stream_output file
with open("QA.txt", "r") as file:
    # create list (each element is a line)
    data = file.readlines()
    # add to tuples the Hashtag and the number of Occurrences
    for i in range(len(data)):
        split_data = data[i].split()
        hashtags += (split_data[0],)
        counts += (int(split_data[1]),)

# plot graph
ypos = np.arange(len(hashtags))
plot.figure()
plot.bar(ypos, counts,color=sns.color_palette("hls",5), align = 'center', alpha = 0.5)
plot.xticks(ypos, hashtags,rotation=45)
plot.ylabel("Occurrences")
plot.xlabel("Hashtags")
plot.title("Hashtags Vs Occurances")
plot.subplots_adjust(bottom = 0.2)
plot.savefig("output")
