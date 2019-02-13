
import re
# number of items in each topic
politics_counter =0
artists_counter = 0
news_counter = 0
televisions_counter = 0
companies_counter = 0

# positive,negative and neutral values for each topic
politics_pos = 0
politics_neg = 0
politics_neu = 0

artists_pos = 0
artists_neg = 0
artists_neu = 0

news_pos = 0
news_neg = 0
news_neu = 0

televisions_pos = 0
televisions_neg = 0
televisions_neu = 0

companies_pos = 0
companies_neg = 0
companies_neu = 0

with open ("storage.txt" , "r") as file:
	data = file.readlines()
	for i in range(len(data)):
		split_data = data[i].split()
		#only get the numbers (return a list so we take the first one)
		neg = float(re.findall("\d*\.\d*",split_data[5])[0])
		pos = float(re.findall("\d*\.\d*",split_data[7])[0])
		neu = float(re.findall("\d*\.\d*",split_data[9])[0])
		# for each topic, we add their repective positive,negative and neutral vales together
		# it keep track of the number of items belong to each topic by using counters
		if split_data[0] =="Politics":
			politics_pos += pos
			politics_neg += neg
			politics_neu += neu
			politics_counter +=1
		if split_data[0] =="Companies":
			companies_pos += pos
			companies_neg += neg
			companies_neu += neu
			companies_counter +=1
		if split_data[0] =="Televisions":
			televisions_pos += pos
			televisions_neg += neg
			televisions_neu += neu
			televisions_counter +=1
		if split_data[0] =="Artists":
			artists_pos += pos
			artists_neg += neg
			artists_neu += neu
			artists_counter +=1
		if split_data[0] =="News":
			news_pos += pos
			news_neg += neg
			news_neu += neu
			news_counter +=1
# write the final result into a text file after aggregating and diving values of each topic
with open ("QB.txt", "w+") as file:
	# we divide the totalpostive, total negative , and total neutral values of each topic by their respective size (counter that kept track of number of items in each topic)
	# format the result to 4 decimal points
	file.write("Politics " + str("{0:.4f}".format(politics_pos/politics_counter)) +" " + str("{0:.4f}".format(politics_neg/politics_counter)) +" " + str("{0:.4f}".format(politics_neu/politics_counter) )+ '\n' )
	file.write("Companies " +str("{0:.4f}".format(companies_pos/companies_counter))+" " +str("{0:.4f}".format(companies_neg/companies_counter))+" " + str("{0:.4f}".format(companies_neu/companies_counter))+ '\n' )
	file.write("Televisions "+ str("{0:.4f}".format(televisions_pos/televisions_counter))+" " +str("{0:.4f}".format(televisions_neg/televisions_counter))+" " + str("{0:.4f}".format(televisions_neu/televisions_counter))+ '\n' )
	file.write("Artists "+str ("{0:.4f}".format(artists_pos/artists_counter))+" " +str("{0:.4f}".format(artists_neg/artists_counter))+" " +str("{0:.4f}".format(artists_neu/artists_counter))+ '\n' )
	file.write("News " +str("{0:.4f}".format(news_pos/news_counter))+" " + str("{0:.4f}".format(news_neg/news_counter))+" " + str("{0:.4f}".format(news_neu/news_counter)) + '\n')

