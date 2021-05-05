# Hadoop Mini-Project Reducer1 Python Script

# Note:
# - Within terminal, we use the pipe operator to work w/
# results from autoinc_mapper.py.
# - I still need to download Hadoop onto my local computer.

import sys

# [Define group level master information]

# Q: What is the purpose of group level master information?
# Q: What is the purpose of the functions reset and flush?


def reset():
    # [Logic to reset master information for every new group.]
    pass


# Run for end of every group
def flush():
    # [Write the output]
    pass


# Read input
for line in sys.stdin:

    # [Parse the input we got from mapper and update the master info]
    mapper = line.split()
    current_vin = mapper[2]
    vin = "SOME VALUE"

    # [Detect key changes]
    if current_vin != vin:
        if current_vin is not None:
            # write result to STDOUT
            flush()
        reset()

    # [Update more master info after the key change handling.]

    current_vin = vin
# Do not forget to output the last group if needed.
flush()
























