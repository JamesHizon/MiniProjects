# A Beginners Introduction into MapReduce

# Intro:
# - Many times as a Data Scientist, we have to deal w/ huge amounts of data.
# - MapReduce technique: a broad technique used to handle large amounts of data.
# - Many implementations include Apache Hadoop.

# Given a list of strings, want to return the longest string:

def find_longest_string(string_list):
    longest_string = None
    longest_string_len = 0

    for s in string_list:
        if len(s) > longest_string_len:
            longest_string_len = len(s)
            longest_string = s

    return longest_string


# We go over strings one by one, compute the length and keep the longest string
# until we finished.

# For small lists, it works pretty fast:

list_of_strings = ["abc", "python", "dima"]
#
# import timeit
# print("The time taken is ", timeit.timeit(stmt="""
# list_of_strings = ["abc", "python", "dima"];
#
# def find_longest_string(string_list):
#     longest_string = None
#     longest_string_len = 0
#
#     for s in string_list:
#         if len(s) > longest_string_len:
#             longest_string_len = len(s)
#             longest_string = s
#
#     return longest_string;
#     find_longest_string(list_of_strings)"""))
#
# # print(find_longest_string(list_of_strings))


# Even for lists w/ much more than 3 elements it works pretty well, here we try w/
# 3000 elements:

# large_list_of_strings = list_of_strings*1000
# print("The time taken is ", timeit.timeit(stmt="""large_list_of_strings = list_of_strings*1000;
# def find_longest_string(string_list):
#     longest_string = None
#     longest_string_len = 0
#
#     for s in string_list:
#         if len(s) > longest_string_len:
#             longest_string_len = len(s)
#             longest_string = s
#
#     return longest_string;
#     find_longest_string(large_list_of_strings)"""))

# print(find_longest_string(large_list_of_strings))

# Two Main Things to do in code:

# 1) Computing length of string and comparing it to the longest string until now.
# 2) We'll bring our code into two steps: 1) Compute len of all strings. 2) Select max value.

# Step 1 - Mapper (Maps some value into some other value):
list_of_string_lens = [len(s) for s in list_of_strings]
list_of_string_lens = zip(list_of_strings,
                          list_of_string_lens)  # Create tuple of list of strings and list of string lengths.

# Step 2 - Reducer (Gets a list of values and produces a single (in most cases) value:
max_len = max(list_of_string_lens, key=lambda t: t[1])
print(max_len)

# OUTPUT:
# ("python", 6)
# CPU times: user 51.6 s, sys: 804 ms, total: 52.4 s

# Two Helper Functions for mapper and reducer

mapper = len


def reducer(p, c):
    if p[1] > c[1]:
        return p
    return c


# Mapper:
# - Just the len function.
# - Gets a string and returns its length.

# Reducer:
# - Gets two tuples as input and returns the one w/ the biggest length.

# Now, we rewrite code using map and reduce.

from functools import reduce

# Step 1:
mapped = map(mapper, list_of_strings)
mapped = zip(list_of_strings, mapped)

# Step 2:
reduced = reduce(reducer, mapped)

print(reduced)

# Note:
# - The code does exactly the same thing. It looks a bit fancier, but also,
# it is more generic and will help us parallelize it. Let's look more closely
# at it.

# Step 1:
# - Maps our list of strings into a list of tuples using a mapper function
# (will use zip again to avoid duplicating strings).

# Step 2:
# - Uses the reducer function, goes over the tuples from step one and applies it
# one by one. The result is a tuple w/ maximum length.

# Now, we want to break out input into chunks and understand how it works before we
# do any parallelization (using "chunkify" to break a large list into chunks of
# equal size).








