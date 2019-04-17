import pandas as pd
import os
import sys

def main():
	print("Start")
	folder = str(os.getcwd()) + "/data/"
	filename = "user.json"

	split_folder = "user/"
	split_name = "user_"

	if(len(sys.argv) > 1):
		filename = sys.argv[1]
		print("File: ", folder + filename)
	else:
		print("Default file: ", folder + filename)

	chunksize = 500000

	reader = pd.read_json(folder + filename, orient = 'records', lines = True, chunksize = chunksize)


	# Setup folder structure
	try:
	    # Create target Directory
	    os.mkdir(folder + split_folder)
	    print("Directory " , folder + split_folder ,  " Created ")
	except FileExistsError:
	    print("Directory " , folder + split_folder ,  " already exists")


	count = 0;
	# Panda DataFrame
	for chunk in reader:
		chunk = chunk.drop(["elite", "yelping_since", "friends", "compliment_cool","compliment_cute","compliment_funny","compliment_hot","compliment_list","compliment_more","compliment_note","compliment_photos","compliment_plain","compliment_profile","compliment_writer"], axis = 1) # We are not concerned with actual review text
		chunk.to_json(folder + split_folder + split_name + str(count) + ".json", orient='records', lines = True)

		print("\tWriting ", str(count))
		count += 1

	print("\nDone")


if __name__ == '__main__':
	main()
