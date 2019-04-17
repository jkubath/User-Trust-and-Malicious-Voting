# small data is used to split the data set into a smaller
# subset of data
# 1. Takes in the large directory of data and a target business id
# 2. Search for all the users who voted on the target business
# 3. finds all the businesses that those users also voted on
# 4. writes their data to business, user, and review folder
import os # file structure
import pandas as pd # DataFrames


# read the reviews and return a list of user_ids who voted on the target business
def findUsersByBusiness(reviews_folder, target_business):
	print("Directory: {}".format(reviews_folder))

	users = []

	# Read all the files in the directory
	for file in os.listdir(reviews_folder):
		# skip directories
		if not os.path.isfile(reviews_folder + file):
			continue

		print("\tReading: {}".format(file))

		# Read data into Pandas DataFrame object
		data = pd.read_json(reviews_folder + file, orient = 'records', lines = True)

		for index, row in data.iterrows():

			# this is not the target business
			if row['business_id'] == target_business:
				# add the user if they aren't already in the list
				if not row['user_id'] in users:
					users.append(row['user_id'])

	# return all the users who voted on the target business
	return users

# read the reviews and return a list of business_ids who were voted on by users
def findBusinessByUser(reviews_folder, users):
	print("Directory: {}".format(reviews_folder))

	cor_business = []

	# Read all the files in the directory
	for file in os.listdir(reviews_folder):
		# skip directories
		if not os.path.isfile(reviews_folder + file):
			continue

		print("\tReading: {}".format(file))

		# Read data into Pandas DataFrame object
		data = pd.read_json(reviews_folder + file, orient = 'records', lines = True)

		for index, row in data.iterrows():
			# user has voted on target business
			if row['user_id'] in users:
				# business not previously added to cor_business
				if not row['business_id'] in cor_business:
					cor_business.append(row['business_id'])

	# list of businesss who were voted on by cor_users
	return cor_business

# find any value in the test_set and write to the output file
def findWrite(input_folder, output_file, test_set, column):
	print("Directory: {}".format(input_folder))

	# Read all the files in the directory
	for file in os.listdir(input_folder):
		# skip directories
		if not os.path.isfile(input_folder + file):
			continue

		print("\tReading: {}".format(file))

		# Read data into Pandas DataFrame object
		data = pd.read_json(input_folder + file, orient = 'records', lines = True)

		for index, row in data.iterrows():
			# this item is in our target set
			if row[column] in test_set:
				output_file.write(row.to_json())
				output_file.write("\n")

# find any review by the users or on the business' and write to output file
def findReviews(reviews_folder, output_file, users, business):
	print("Directory: {}".format(reviews_folder))

	# Read all the files in the directory
	for file in os.listdir(reviews_folder):
		# skip directories
		if not os.path.isfile(reviews_folder + file):
			continue

		print("\tReading: {}".format(file))

		# Read data into Pandas DataFrame object
		data = pd.read_json(reviews_folder + file, orient = 'records', lines = True)

		for index, row in data.iterrows():

			if row['business_id'] in business or row['user_id'] in users:
				output_file.write(row.to_json())
				output_file.write("\n")

# 1. Find the users who voted on the target business
# 2. Find the business' reviewed by those users
# 3. Write user's and business' json data
# 4. Find the reviews by any of those users or on any of the business' in the sets
def findData(folder, business_folder, reviews_folder, users_folder, output_folder, target_business):

	# find all users who voted on the target business
	cor_users = findUsersByBusiness(folder + "/" + reviews_folder, target_business)
	print("Found {} cor_users".format(len(cor_users)))

	# find all business voted by cor_users
	cor_business = findBusinessByUser(folder + "/" + reviews_folder, cor_users)
	print("Found {} cor_business".format(len(cor_business)))

	# write the business data
	output_business = open(output_folder + business_folder + "business.json", "w")
	findWrite(folder + "/" + business_folder, output_business, cor_business, "business_id")

	# write the user data
	output_user = open(output_folder + users_folder + "user.json", "w")
	findWrite(folder + "/" + users_folder, output_user, cor_users, "user_id")

	# write review data
	output_reviews = open(output_folder + reviews_folder + "review.json", "w")
	findReviews(folder + "/" + reviews_folder, output_reviews, cor_users, cor_business)

def main():
	print("Small data")

	folder = str(os.getcwd())
	# folder = str(os.getcwd()) + "/test" # test root folder
	business_folder = "business/"
	reviews_folder = "reviews/"
	users_folder = "user/"

	output_folder = str(os.getcwd()) + "/small_data_001/"

	# target_business = "DPhHRzXVoOWSJydsyPKL1g" # yelp business id top %10 business 4 votes
	# target_business = "xmQYvUV-LmotddwQYNtkzQ" # top %0.05 business 54 votes
	target_business = "iBPyahdJRP5y0t25fF2W9w" # top business 257 votes
	# target_business = "business1" # test business

	os.makedirs(output_folder + business_folder, exist_ok=True)
	os.makedirs(output_folder + reviews_folder, exist_ok=True)
	os.makedirs(output_folder + users_folder, exist_ok=True)

	findData(folder, business_folder, reviews_folder, users_folder, output_folder, target_business)




if __name__ == '__main__':
	main()
