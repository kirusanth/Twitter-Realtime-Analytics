FOR PART A
--------------------

run following commands in terminal 1:
	1.docker run -it -v ${PWD}:/app --name twitter -p 9009:9009 python bash
	2.pip install -U git+https://github.com/tweepy/tweepy.git@2efe385fc69385b57733f747ee62e6be12a1338b
	3.python twitterQA.py ---> it will generate tweets live

run following command in terminal 2:
	1.docker run -it -v ${PWD}:/app --link twitter:twitter eecsyorku/eecs4415 
	2.pip install nltk ----------> needed sparkQA.py
	3.spark-submit sparkQA.py ---------> it will create QA.txt 

	wait until you get enough records in QA.txt

	5.pip install matplotlib ------->needed for plotQA
	6.pip install seaborn -----------> needed for plotQA
	7.python plotQA  ----> it will open QA.text and read the line and plot the barcharts in output.png




FOR PART B
--------------------

run following commands in terminal 1:
	1.docker run -it -v ${PWD}:/app --name twitter -p 9009:9009 python bash
	2.pip install -U git+https://github.com/tweepy/tweepy.git@2efe385fc69385b57733f747ee62e6be12a1338b
	3.python twitterQB.py ---> it will generate tweets live
run following command in terminal 2:
	1.docker run -it -v ${PWD}:/app --link twitter:twitter eecsyorku/eecs4415 
	2.pip install nltk ---> needed for sparkQB
	3.spark-submit sparkQB.py ---> it will create storageQB.txt

	wait until you get enough records in StorageQB.txt

	4.python combinerQB.py --> it will open storageQB.txt file and read lines then produce QB.txt
	5.pip install matplotlib --->  needed for plotQB
	7.python plotQB -----> it will open QA.text and read the line and plot the barcharts in output.png
