# Intro to Hadoop - Creating a Mapper Notes

# Learning Objectives:
# - Understand the process of viewing data in terms of Key/Value pair.
# - Understand the process of going backward from the desired answer to the logic
# for reducer and then mapper.
# - Create python-based mapper function and test against non-Hadoop input.

# Example:
# - Have a ratings.csv file.
# - Have a mapper01.py, mapper02.py and mapper03.py file.


# mapper01.py

# import sys
# for oneMovie in sys.stdin:
#     oneMovie = oneMovie.strip()
#     ratingInfo = oneMovie.split(",")
#     movieID = ratingInfo[1]
#     rating = ratingInfo[2]
#     print(f"{}\t{}".format(movieID, rating))

# mapper03.py
# import sys
# import csv
#
# # Using movieFile, we create a movieList object to store title and genre data.
# movieFile = "movies.csv"
# movieList = {}
# with open(movieFile, "r") as infile:
#     reader = csv.reader(infile)
#     for row in reader:
#         movieList[row[0]] = {}
#         movieList[row[0]]["title"] = row[1].strip()
#         movieList[row[0]]["genre"] = row[2].strip()
# for oneMovie in sys.stdin:
#     oneMovie = oneMovie.strip()
#     ratingInfo = oneMovie.split(",")
#     try:
#         movieTitle = movieList[ratingInfo[1]]["title"]
#         rating = float(ratingInfo[2])
#         print(f"{}\t{}".format(movieTitle, rating))
#     except ValueError:
#         continue

# Bash Shell:
# !ssh dsciu001 hdfs -cat /repository/movielens/ratings.csv 2>/dev/null | head -n 10 | python mapper03.py

# Output:
# Free Willy 2: The Adventure Home (1995) 2.5
# ...
# Prince of Egypt... 4.0
















