import pandas as pd # DataFrames
import os # file system
import sys # access to argv
from multiprocessing import Pool # Read review files in parallel
import operator
import numpy as np

# VERBOSE LEVEL
# 1
# 2 - print UpdateSimulation, Trap, TrapCorrelated, VoteHonest
# 3 - print readBusinessData, readReviewData, addMaliciousVotes
# 4 - print updateBusinessScore
# 5 - print user trust scores when calculating business stars
VERBOSE = 1
TEST = False
review_files = 2

# Dictionary for all the users
# key = user_id
all_users = {}
# all_users_list = [] # sorted after each iteration by trust score, then # of good votes
# Dicionary for all the business'
# key = business_id
all_business = {}
# all_business_list = [] # sorted after each iteration by stars, then review_count
all_user_count = 0
all_business_count = 0

malicious_votes = {} # holds the business' and the malicious users who voted for that business
new_malicious_votes = []

malicious_users = 240
malicious_id_base = "malicious_"

# Class for all the user's data
class User:
	def __init__(self):
		self.trust = 0.5
		self.good = 0 # business' that match our vote
		self.bad = 0 # business' that go against our vote
		self.good_votes = [] # votes that match the crowd vote
		self.bad_votes = [] # votes that go against the crowd vote
		return

	def addGood(self):
		self.good += 1

	def addBad(self):
		self.bad += 1

	def printData(self):
		print("{}".format(self.trust))

# Class for all the business' data
class Business:
	name = ""
	stars = 3
	review_count = 0
	good = 0.0 # user votes > 3
	bad = 0.0 # user votes < 3
	undecided = 0.0 # user votes == 3
	user_review_good = {} # Set holds the users who reviewed this business positively
	user_review_bad = {} # Set holds the users who reviewed this business poorly

	def __init__(self, name, stars, review_count):
		self.name = name
		self.stars = stars
		self.review_count = review_count
		self.user_review_good = set()
		self.user_review_bad = set()

	def printData(self):
		print('{} {} {}'.format(self.name, self.stars, self.review_count))

	def addGood(self, trust):
		self.good += trust

	def addBad(self, trust):
		self.bad += trust

	def addUndecided(self, trust):
		self.undecided += trust

	def addUserReview(self, user, good):
		if good:
			self.user_review_good.add(user)
		else:
			self.user_review_bad.add(user)

# Read from the JsonReader object data
# dictionary:
# 	key: business_id, value: business object
def readBusinessData(data):
	global all_business_count
	global all_business
	count = 0

	# Iterate over business data
	for index, row in data.iterrows():
		# business does not exist
		if all_business.get(row['business_id'], -1) == -1:

			all_business[row['business_id']] = Business(row['name'], row['stars'], row['review_count'])
			all_business_count += 1

		count += 1

		if(VERBOSE >= 3 and count % 50000 == 0):
			print("Reading {} business'".format(count))

# Read from the JsonReader Object data
def readReviewData(reviewFile):
	global all_user_count
	global all_users

	if VERBOSE >= 3:
		print("Reading {}".format(reviewFile))
	# Read data into Pandas DataFrame object
	data = pd.read_json(reviewFile, orient = 'records', lines = True)

	# Iterate over the review data
	count = 0
	for index, row in data.iterrows():
		# User does not exist
		if all_users.get(row['user_id'], -1) == -1:

			all_users[row['user_id']] = User()
			all_user_count += 1

		# Record the vote
		# Error checking for if the business_id doesn't exist
		if all_business.get(row['business_id'], -1) == -1:
			print("Business {} does not exist".format(row['business_id']))
		else:
			user_trust = all_users.get(row['user_id']).trust
			if row['stars'] < 3:
				all_business[row['business_id']].addBad(user_trust)			# Bad Vote
			elif row['stars'] == 3:
				all_business[row['business_id']].addUndecided(user_trust)	# Undecided Vote
			else:
				all_business[row['business_id']].addGood(user_trust)		# Good Vote

			#print("business good {}".format(all_business[row['business_id']].good))
			good = False
			if row['stars'] >= 3:
				good = True
			# add the user to the set of users that reivewed that business
			all_business[row['business_id']].addUserReview(row['user_id'], good)

		count += 1

		if(VERBOSE >= 3 and count % 100000 == 0):
			print("Reading {} users' reviews".format(count))

# Read the vote data from malicious users and add it to the business' votes
def addMaliciousVotes(votes):
	global all_business
	global all_users

	if VERBOSE >= 3:
		print("\taddMaliciousVotes")
	# iterate over all the malicious votes
	count = 0
	for vote_list in votes:
		vote = vote_list[0]
		# print(vote)
		# Record the vote
		# Error checking for if the business_id doesn't exist
		if all_business.get(vote['business_id'], -1) == -1:
			print("Business {} does not exist".format(vote['business_id']))
		else:
			user_trust = all_users.get(vote['user_id']).trust
			if vote['stars'] < 3:
				all_business[vote['business_id']].addBad(user_trust)		# Bad Vote
			elif vote['stars'] == 3:
				all_business[vote['business_id']].addUndecided(user_trust)	# Undecided Vote
			else:
				all_business[vote['business_id']].addGood(user_trust)		# Good Vote

			#print("business good {}".format(all_business[vote['business_id']].good))
			good = False
			if vote['stars'] >= 3:
				good = True
			# add the user to the set of users that reivewed that business
			all_business[vote['business_id']].addUserReview(vote['user_id'], good)

		# Add the additional vote to the user's good votes
		# all_users[vote['user_id']].addGood()

		count += 1

	if(VERBOSE >= 3):
		print("\tAdded {} malicious votes".format(count))

# call the .printData() in the data object with class name 'name'
def printDictionary(data, name, max = -1):
	print("\nPrinting: " + name)
	count = 0
	for key, value in data.items():
		#print(key)
		#print("\t", end =""), # print tab without newline
		value.printData()
		count += 1
		# Cut off for printing
		if max != -1 and count >= max:
			return

# updates the business score based on total user trust for either good or bad reviews
# then iterates through the sets of users who voted good or bad and updates their
# good / bad votes based on whether or not it matches the population vote
def updateBusinessScore(business_id, business):
	global all_business
	global all_users

	business.good = 0
	business.bad = 0
	for user in business.user_review_good:
		if VERBOSE >= 5:
			print("\t\t{} {}".format(user, all_users[user].trust))
		business.good += all_users[user].trust
	for user in business.user_review_bad:
		if VERBOSE >= 5:
			print("\t\t{} {}".format(user, all_users[user].trust))
		business.bad += all_users[user].trust

	good = False
	totalStars = 5.0
	totalVotes = business.good + business.undecided + business.bad

	if totalVotes > 0:
		percentage = (business.good + business.undecided / 2) / totalVotes
	else:
		#print("{} has no votes".format(business.name))
		# no votes on this business so continue
		return

	if VERBOSE >= 4:
		print("{} - {}".format(business_id, business.good))

	# user trust with good reviews >= user trust with bad reviews
	if business.good >= business.bad:
		good = True

	business.stars = totalStars * percentage

	# list of users who voted on this business
	users = []
	if good:
		users = list(business.user_review_good)
		# Good votes match this business
		for user in business.user_review_good:
			all_users[user].addGood()
			all_users[user].good_votes.append(business_id) # user vote matches the crowd
		# Bad votes don't match
		for user in business.user_review_bad:
			all_users[user].addBad()
			all_users[user].bad_votes.append(business_id) # user vote goes against the crowd
	else:
		users = list(business.user_review_bad)
		# Bad votes match this business
		for user in business.user_review_bad:
			all_users[user].addGood()
			all_users[user].good_votes.append(business_id)
		# Good votes don't match
		for user in business.user_review_good:
			all_users[user].addBad()
			all_users[user].bad_votes.append(business_id)

	return users

# Reset good, bad, and undecided for all business
def resetBusinessVotes():
	global all_business

	# Reset good, bad, and undecided votes for business'
	for key, business in all_business.items():
		business.good = 0
		business.bad = 0
		business.undecided = 0
		# business.user_review_good.clear()
		# business.user_review_bad.clear()

# Reset good and bad for all users except malicious_ids
# skip malicious votes because they are not written to file so clearing them
# would erase the vote
def resetUserVotes():
	global all_users

	# Reset good and bad for users
	for key, user in all_users.items():
		user.good = 0
		user.bad = 0

# updates the user's trust score
# Input: user object
# good = reviews that match the population reviews
# bad = reviews that don't match the population reviews
# Trust(user) = (good + 1) / (good + bad + 2)
def updateUserScore(user_id, user):
	global all_business
	global all_users

	startScore = user.trust
	totalReviews = user.good + user.bad

	# if user_id == "malicious_0":
	# 	print("START: {} {}".format(startScore, user.good))

	if totalReviews == 0:
		# print("{} has no reviews".format(user_id))
		# user has not voted
		return 0.0
	else:
		user.trust = (user.good + 1) / (totalReviews + 2)

	# if user_id == "malicious_0":
	# 	print("END: {}".format(user.trust))

	# return the change in trust score
	# print("{} {}".format(user_id, user.trust))
	return abs(user.trust - startScore)

# Create a directory for output
def createOutputDir(folderPath = "C:/yelp_output/"):
	try:
		os.makedirs(folderPath, exist_ok=True) # succeeds even if directory exists
	except FileExistsError:
    	# directory already exists
		pass

# write the trust data for business and users to the folder
def outputTrust(loopCount, all_users_list, all_business_list, folder = "C:/yelp_output/"):
	# global all_business_list
	# global all_users_list

	createOutputDir(folder)
	businessObject = open(folder + str(loopCount) + "_yelp_business", "w", encoding="utf-8")
	userObject = open(folder + str(loopCount) + "_yelp_users", "w", encoding="utf-8")

	string = ""
	# print business data to file
	# for key, business_data in all_business:
	for key in all_business_list:
		business_data = all_business[key]
		string = "{},{},{:.3f},{}\n".format(key, business_data.name, business_data.good, business_data.review_count)
		# print(string)
		businessObject.write(string)

	# print user data to file
	for key in all_users_list:
		string = "{},{:.06f}\n".format(key, all_users[key].trust)
		userObject.write(string)

# users is a list of user_ids that voted on the target business
# correlated set will be a set of all the other businesses that user's vote
# matched the crowd vote.  This will allow the RepTrap to "flip" these business'
# star rating to decrease the honest users and increase the malicious users trust score
def calculateCorrelated(users, targets):
	global all_business
	global all_users

	if not users or len(users) == 0:
		print("No correlated users")
		return []

	cor_set = {}
	for user in users:
		for business_id in all_users[user].good_votes:
			# It is already a given that users have voted on the target business
			if business_id in targets:
				continue
			# Add the business and user list to correlated set
			if business_id not in cor_set:
				# print("Adding {}".format(all_business[business_id].name))
				cor_set[business_id] = [user]
			# Add the user to the correlated set
			else:
				cor_set[business_id].append(user)

	# sort the dictionary by the value length of users per business
	sorted_keys = sorted(cor_set, key = lambda k: len(cor_set[k]), reverse=True)
	# sorted_list = []
	# for k in sorted_keys:
	# 	sorted_list.append([k, cor_set[k]])
	# return sorted_list
	# return sorted(cor_set.items(), key = lambda k: len(k[1]), reverse=True)
	return sorted_keys

# Weight = (correlated users) / business trust
# correlated users = number of users who voted on business and target
# business trust = abs(business.good - business.bad)
def calculateCorrelatedSort(business, target):
	business_users = []
	target_users = []

	if all_business[business].stars >= 3:
		business_users = all_business[business].user_review_good
	else:
		business_users = all_business[business].user_review_bad

	if all_business[target].stars >= 3:
		target_users = all_business[target].user_review_good
	else:
		target_users = all_business[target].user_review_bad

	correlated = 0
	if len(business_users) > len(target_users):
		for i in target_users:
			if i in business_users:
				correlated += 1
	else:
		for i in business_users:
			if i in target_users:
				correlated += 1

	business_trust = abs(all_business[business].good - all_business[business].bad)

	# print("Correlated: {} {:.2f}".format(correlated, business_trust))

	return float(correlated) / business_trust

# Calculate the theoretical gain from strictly voting honestly
# This can be used when the number of votes to vote honestly would
# be less than the number of votes to trap a buiness
def calculateTheorGain(malicious_ids, target, uncor_set):
	print("Calculating theoretical gain")
	new_votes = {} # dictionary of users and how many new votes they add

	# Check to see if it is even possible to get there by voting honestly
	# if all malicious users have %100 trust, and we still can't get the business
	if len(malicious_users) <= business_trust:
		return -1

	# Sort by trust score to optimize benefit
	malicious_ids = sorted(malicious_ids, key = lambda k: all_users[k].trust)

	vote_count = 0
	# What amount of voting trust do we need
	remaining = abs(all_business[target].good - all_business[taget].bad)
	for business in uncor_set:

		# record the votes to trap that business
		for user in malicious_ids:
			# check if the user already voted to trap this business
			if business in malicious_votes:
				if user in malicious_votes[business]:
					continue
			# add to total honest votes from all users
			vote_count += 1

			# keep track of how many honest votes this user has submitted
			if user not in new_votes:
				new_votes[user] = 1
			else:
				new_votes[user] += 1

			# calculate the change in trust score for this user
			user_votes = new_votes[user]
			# good votes + any already made malicious votes + 1 new vote
			user_good = all_users[user].good + user_votes
			user_bad = all_users[user].bad
			user_trust = (user_good-1 + 1) / (user_good-1 + user_bad + 2)
			# good votes + bad votes + 2 + 1 new vote
			user_total = user_good + user_bad

			# change in trust score if we vote honestly once
			# change_in_trust = (previous good votes + 1 new vote + 1) / (previous votes + honest votes + 2)
			change_in_trust = ((user_good + 1) / (user_total + 2)) - user_trust

			# change remaining trust to flip target
			remaining -= change_in_trust

			# Malicious users have enough trust to overcome the honest users
			if remaining < 0:
				finished_voting = True
				break
		# Malicious users have enough trust
		if finished_voting:
			break

	if vote_count > 0:
		print("Theoretical votes: {}".format(vote_count))
		return vote_count
	else:
		return -1

# Sort the malicious_ids by trust score
# If the user has not voted on this business, they can vote
# reverse = True when attempting to trap the target so the highest trust values
# are used first
# All other times we want to trap with the lowest trust to maximize trust gained
def Trap(business_id, malicious_ids, stars, reverse = False):
	global all_business
	global all_users
	global new_malicious_votes

	# Trap the business to bad
	business_trust = abs(all_business[business_id].good - all_business[business_id].bad)

	malicious_trust = 0.0
	total_votes = 0
	votes = [] # list of json votes
	new_votes = [] # list of users voting on the business_id

	if VERBOSE >= 2:
		print("\tTrap {} with trust {:.2f} stars: {:.2f}".format(business_id, business_trust, all_business[business_id].stars))

	# sort malicious users from low to high trust
	# sorted_mal = sorted(malicious_users, key = lambda k: all_users[k].trust)
	sorted_mal = sorted(malicious_ids, key = lambda k: all_users[k].trust, reverse=reverse)
	index = 0 # index of sorted_mal array
	# iterate entire array and while malicious trust is <= business_trust
	while index < len(malicious_ids) and malicious_trust <= business_trust:

		# the current user has not voted on this business
		user = sorted_mal[index]
		if VERBOSE >= 2:
			print("\t{} voted on {} with trust {:.2f} stars: {}".format(user, business_id, all_users[user].trust, stars))
		malicious_trust += all_users[user].trust

		votes.append([{	"user_id" : user,
						"business_id" : business_id,
						"stars" : stars}])

		# record this user voting on this business
		new_votes.append(user)
		index += 1
		total_votes += 1

		# trap the business with fewest malicious votes
		if malicious_trust > business_trust:
			break



	# only add the votes if the business can be trapped
	if malicious_trust > business_trust:
		new_malicious_votes.extend(votes) # add to the array of malicious votes
		if business_id in malicious_votes:
			malicious_votes[business_id].append(new_votes)
		else:
			malicious_votes[business_id] = new_votes

		return total_votes

	# all the users have voted on this business, remove it from set
	elif total_votes == 0:
		return -2
	else:
		return -1

# Iterate through the cor_set and attempt to trap the first available business
def TrapCorrelated(cor_set, target, malicious_ids, malicious_trust):
	global all_business
	global all_users

	if len(cor_set) == 0:
		return "", -2, []

	total_votes = 0
	votes = 0
	remove_business = [] # business' that can no longer be trapped or have been voted on by all malicious users
	cor_sorted = sorted(cor_set, key = lambda k: calculateCorrelatedSort(k, target), reverse=True)

	# print the weight of the top 5 correlated businesses
	# print("Length of cor_sorted: {}".format(len(cor_sorted)))
	# count = 0
	# index = 0
	# while index < len(cor_sorted) and count < 5:
	# 	cor = calculateCorrelatedSort(cor_sorted[index], target)
	# 	# if not int(cor) == 0:
	# 	print("sorted: {} {:.2f}".format(cor_sorted[index], cor))
	# 	count += 1
	# 	index += 1

	# business = list of business name and a list of users who voted
	# business = [business_name, [user1, user2, user3]]
	for business in cor_sorted:
		# calculateCorrelatedSort == 0 when no users voted correctly on
		# the business, but voted correctly on the target business
		cor_score = calculateCorrelatedSort(business, target)
		if cor_score == 0:
			continue

		business_trust = abs(all_business[business].good - all_business[business].bad)
		# How can we trap this business
		# good -> bad or bad -> good
		# we can't trap this business
		if malicious_trust < business_trust:
			continue

		if business in malicious_votes:
			if VERBOSE >= 2:
				print("\t{} was previously trapped".format(business))
			continue

		business_stars = all_business[business].stars

		stars = 0
		# current business is bad, we want to flip it to good
		if business_stars < 3:
			stars = 5

		# record the votes to trap that business
		votes = Trap(business, malicious_ids, stars)

		# trap was successful
		if votes > 0:
			return business, votes, [business]
		# all the malicious users have voted on this item, remove it
		# elif votes == -2:
		# 	remove_business.append(business)

	# all malicious users have voted on all the business' in the set
	# if len(remove_business) > 0:
	# 	return "", -3, remove_business

	# could not trap any business
	return "", -1, []

# Vote honestly on the business in the set
def VoteHonest(uncor_set, malicious_ids, remaining):
	global all_business
	global all_users

	if len(uncor_set) == 0:
		return -2, remaining

	# Sort by trust score to optimize benefit
	malicious_ids = sorted(malicious_ids, key = lambda k: all_users[k].trust)

	vote_count = 0
	finished_voting = False
	votes = [] # list of honest vote data
	new_votes = {} # dictionary of users and how many new votes they add
	# business = list of business name
	for business in uncor_set:
		business_stars = all_business[business].stars
		# print("{}".format(business))

		stars = 0
		# current business is good, we vote good
		if business_stars >= 3:
			stars = 5

		# record the votes to trap that business
		for user in malicious_ids:
			# check if the user already voted on this business
			if business in malicious_votes:
				if user in malicious_votes[business]:
					continue

			if VERBOSE >= 2:
				print("\tHonest vote {} - {} {:.2f}".format(user, business, remaining))

			# add to total honest votes from all users
			vote_count += 1

			# keep track of how many honest votes this user has submitted
			if user not in new_votes:
				new_votes[user] = 1
			else:
				new_votes[user] += 1

			# calculate the change in trust score for this user
			user_votes = new_votes[user]
			# good votes + any already made malicious votes + 1 new vote
			user_good = all_users[user].good + user_votes
			user_bad = all_users[user].bad
			user_trust = (user_good-1 + 1) / (user_good-1 + user_bad + 2)
			# good votes + bad votes + 2 + 1 new vote
			user_total = user_good + user_bad

			# change in trust score if we vote honestly once
			# change_in_trust = (previous good votes + 1 new vote + 1) / (previous votes + honest votes + 2)
			change_in_trust = ((user_good + 1) / (user_total + 2)) - user_trust
			print("\tChange: {:.2f} {} {}".format(change_in_trust, user_good, user_bad))
			# add vote to list
			votes.append([{	"user_id" : user,
							"business_id" : business,
							"stars" : stars}])
			# change remaining trust to flip target
			remaining -= change_in_trust

			# Malicious users have enough trust to overcome the honest users
			if remaining < 0:
				finished_voting = True
				break
		# Malicious users have enough trust
		if finished_voting:
			break

	if vote_count > 0:
		new_malicious_votes.extend(votes) # add new malicious votes

	return vote_count, remaining

# update the business and user trust scores
def UpdateSimulation(folder, target, create_malicious, readReviews = False):
	malicious_ids = []
	loopCount = 0
	printTrust = False
	review_folder = folder + "/reviews/"
	# Loop through the reviews while any change in user trust score is above delta
	delta = 0.1 # values from 0.0 to 1.0
	continueLoop = True # Loop through at least once
	biggestChange = 0.0 # Initialize to 100% change

	while(continueLoop):
		# reset the number of good / bad votes for users
		# resetUserVotes()
		# reset the number of good / bad votes for business
		resetUserVotes()
		resetBusinessVotes()

		# Read the reviews and add good or bad to the businesses
		if readReviews:
			# count = 0
			for file in os.listdir(review_folder):
				readReviewData(os.path.join(review_folder + file))
				# count += 1
				# if count == review_files:
				# 	break;

		addMaliciousVotes(new_malicious_votes)

		# create the malicious users
		if create_malicious:
			for i in range(malicious_users):
				all_users[malicious_id_base + str(i)] = User()
				malicious_ids.append(malicious_id_base + str(i))
			create_malicious = False

		# Calculate business stars and update user good / bad variables
		# good++ if their vote matches / ties the business score
		# bad++ if their vote doesn't match the business score
		if(VERBOSE > 4):
			print("Updating business' score")

		targetUsers = [] # holds the users who voted on the target business
		# key = business_id
		# business = business object
		for key, business in all_business.items():
			#print("{} has {} good votes".format(business.name, business.good))

			# We found the target business, get the set of users who voted on it
			# for the correlated set
			if key == target:
				targetUsers = updateBusinessScore(key, business)
			else:
				updateBusinessScore(key, business)

		# Calculate the user trust score
		# good = reviews that match population vote
		# bad = reviews that don't match population vote
		# Trust(user) = (good + 1) / (good + bad + 2)
		if(VERBOSE > 4):
			print("Updating user's trust score")

		biggestChange = 0.0
		for key, user in all_users.items():
			change = updateUserScore(key, user)
			if  change > biggestChange:
				biggestChange = change

		if biggestChange < delta:
			continueLoop = False
		# else:
			# print("Biggest Change: {:.2f}".format(biggestChange))

		# print("Loop Count {}".format(loopCount))

		loopCount += 1

	print("-------------------------------------------------------------------")
	print("Stopped {} with smallest change {:.2f}".format(loopCount, biggestChange))

	# print the status once data has converged
	if printTrust:
		print("Writing to business / user data to file")

		# sort data list
		all_users_list = sorted(all_users, key = lambda user: (all_users[user].trust, all_users[user].good), reverse=True)
		# all_business_list = sorted(all_business, key = lambda business_id: (all_business[business_id].stars, all_business[business_id].review_count), reverse=True)
		all_business_list = sorted(all_business, key = lambda business_id: (all_business[business_id].good, all_business[business_id].review_count), reverse=True)

		# takes in business and user lists and a loopCount index
		outputTrust(loopCount, all_users_list, all_business_list, folder + "/yelp_output/")

	return targetUsers, malicious_ids

# attempt to trap the target business
# 1. calculate malicious_users trust and target business trust
# 2. attempt to directly trap the target
# 3. trap correlated set
# 4. trap uncorrelated set
# 5. vote honestly on uncorrelated
# 6. vote honestly on correlated
def RepTrap(folder, targetUsers, malicious_ids, target):
	global all_business
	global all_users

	# calculate the correlated set of business
	# correlated set: business' who were also voted on by users who voted on the target business
	cor_set = calculateCorrelated(targetUsers, [target])
	uncor_set = np.setdiff1d([*all_business], cor_set + [target]) # [*all_business] returns list of keys

	print("cor: {}".format(len(cor_set)))
	print("un_cor: {}".format(len(uncor_set)))

	malicious_votes = 0 # malicious user votes count
	votes = 0 # temporary holder
	firstRun = True
	iterations = 0
	target_trust = 0
	while True:
		print("Iteration {}".format(iterations))
		iterations += 1

		# nothing to update on the first run
		if not iterations == 1:
			resetUserVotes()
			UpdateSimulation(folder, target, False)

		print("1. Calculations")
		# 1. calculate A_x = trust of malicious users
		malicious_trust = 0.0
		for i in range(malicious_users):
			user = malicious_id_base + str(i)
			# print("\t{} : {:.2f}".format(user, all_users[user].trust))
			malicious_trust += all_users[malicious_id_base + str(i)].trust

		print("\tMalicious trust: {:.2f}".format(malicious_trust))

		# 2. calculate G(target item), if A_x > G(target item) then attack it
		target_trust = 0
		# flip good to bad
		if all_business[target].stars >= 3:
			target_trust = all_business[target].good - all_business[target].bad
		# flip bad to good
		else:
			target_trust = all_business[target].bad - all_business[target].good
		print("\tTarget item good: {:.2f} bad: {:.2f}".format(all_business[target].good, all_business[target].bad))

		# resetBusinessVotes()

		print("2. Trap Target")
		# can we trap with target item without trapping other items
		# attempt to flip the business from good to bad
		if all_business[target].stars >= 3 and malicious_trust > target_trust:
			votes = Trap(target, malicious_ids, 0, True)
			if votes > 0:
				malicious_votes += votes
				print("--Success votes: {}".format(malicious_votes))
				return # exit
		# attempt to flip the business from bad to good
		elif all_business[target].stars < 3 and malicious_trust > target_trust:
			votes = Trap(target, malicious_ids, 5)
			if votes > 0:
				malicious_votes += votes
				print("--Success votes: {}".format(malicious_votes))
				return # exit
		else:
			print("\tFailed to directly trap target")

		# list of business' that have been voted on by all malicious_users
		remove_business = []

		print("3. Trap Correlated")
		# 3. else for items in correlated set, if A_x > G(correlated item), attack it
		business_id, votes, remove_business = TrapCorrelated(cor_set, target, malicious_ids, malicious_trust)
		if votes > 0:
			malicious_votes += votes
			# cor_set.remove(business_id) # remove the trapped item from the set
			print("\tRemoving {} from set".format(remove_business))
			# cor_set = np.setdiff1d(cor_set, remove_business)
			continue # start process again
		elif votes == -2:
			print("\tEmpty correlated set")
		elif votes == -3:
			print("\tNo business' to trap")
		else:
			print("\tFailed to trap correlated")
		# If any business have been voted on by all malicious users, remove them
		# if len(remove_business) > 0:
		# 	print("\tRemoving {} from set".format(remove_business))
		# cor_set = np.setdiff1d(cor_set, remove_business)

		print("4. Trap Uncorrelated")
		# 4. for items in uncorrelated set if A_x > G(uncorrelated item), attack it
		business_id, votes, remove_business = TrapCorrelated(uncor_set, target, malicious_ids, malicious_trust)
		if votes > 0:
			malicious_votes += votes
			# uncor_set = np.setdiff1d(uncor_set, [business_id]) # remove the trapped item from the set
			print("\tRemoving {} from set".format(remove_business))
			continue # start process again
		elif votes == -2:
			print("\tEmpty uncorrelated set")
		elif votes == -3:
			print("\tNo business' to trap")
		else:
			print("\tFailed to trap uncorrelated")
		# If any business have been voted on by all malicious users, remove them
		# if len(remove_business) > 0:
		# 	print("\tRemoving {} from set".format(remove_business))
		# uncor_set = np.setdiff1d(uncor_set, remove_business)

		# Remaining trust to gain by voting honestly
		remaining = target_trust - malicious_trust
		print("5. Vote Uncorrelated")
		print("\tmal: {:.2f} rem: {:.2f}".format(malicious_trust, remaining))
		# 5. vote honestly for any remaining items in the uncorrelated set
		# while remaining > 0 and len(uncor_set) > 0:
		votes, remaining = VoteHonest(uncor_set, malicious_ids, remaining)
		if votes > 0:
			malicious_votes += votes
			# uncor_set = np.setdiff1d(uncor_set, [business_id]) # remove the trapped item from the set
			# break
		elif votes == -2:
			print("\tEmpty uncor_set")
			# uncor_set = []
				# break
			# else:
				# break

		# UpdateSimulation
		if remaining < 0:
			continue
		else:
			print("\tRemaining: {:.2f}".format(remaining))

		print("6. Vote Correlated")
		# 6. return unable to attack item
		# while remaining > 0 and len(cor_set) > 0:
		votes, remaining = VoteHonest(cor_set, malicious_ids, remaining)

		if votes > 0:
			malicious_votes += votes
			# cor_set.remove(business_id) # remove the trapped item from the set
		elif votes == -2:
			print("\tEmpty cor_set")
			# cor_set = []

		# UpdateSimulation
		if remaining < 0:
			continue

		print("Vote: {}".format(malicious_votes))
		print("Business trust: {:.2f}\tMalicious Trust: {:.2f}".format(target_trust, target_trust - remaining))
		print("---------------------------------------------------------------")
		print("Failure")
		return

def main():
	print("Start")
	# folder = str(os.getcwd()) + "/small_data"
	# folder = str(os.getcwd()) + "/small_data_05"
	folder = str(os.getcwd()) + "/small_data_001"

	if TEST:
		folder += "/test/"

	business_folder = folder + "/business/"

	# Read business information
	for file in os.listdir(business_folder):
		# Read business data into dictionary
		print("Reading: {}".format(str(os.path.join(business_folder + file))))
		data = pd.read_json(os.path.join(business_folder + file), orient = 'records', lines = True)
		readBusinessData(data)

	# target = "ujmEBvifdJM6h6RLv4wQIg"
	# target = "iBPyahdJRP5y0t25fF2W9w" # highest rated
	# target = "DPhHRzXVoOWSJydsyPKL1g" # top %10 business (19,000) 4 reviews
	# target = "xmQYvUV-LmotddwQYNtkzQ" # top %0.05 business (100) 54 reviews
	target = "iBPyahdJRP5y0t25fF2W9w" # top business with 257 reviews
	create_malicious = True

	if TEST:
		target = "business1"
		malicious_users = 2


	# Create a base of user scores and business scores
	targetUsers, malicious_ids = UpdateSimulation(folder, target, create_malicious, True)

	print("\tTarget item good: {:.2f} bad: {:.2f}".format(all_business[target].good, all_business[target].bad))
	print("Initialization done")
	print("-------------------------------------------------------------------")
	# Call RepTrap attack
	RepTrap(folder, targetUsers, malicious_ids, target)


	# printDictionary(all_users, "all_users", 10)
	# printDictionary(all_business, "all_business", 10)

	# print("Users {}".format(all_user_count))
	# print("Business {}".format(all_business_count))

if __name__ == '__main__':
	main()
