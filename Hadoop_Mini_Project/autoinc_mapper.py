# Hadoop Mini-Project Mapper Python Script

# Intro:
# - We only care about accident records, so we should filter out records
# having Incident Type I and R.
# - However, accident records do not carry the make and year as they're
# designed to remove redundancy.

# Task:
# - To propagate make and year info from Incident Type I
# into all other record types.

import sys

# Read from standard input
for line in sys.stdin:
    # Create key-value pairs of all values to append to list
    line = line.strip()
    incident_id, incident_type, vin_number, make, model, year, incident_date, description = line.split(",")
    values_list = [incident_type, vin_number, make, year]
    print(f"{incident_type}\t{vin_number}\t{make}\t{year}")

# Okay, so we created a mapper_list w/ list of dictionaries.
# We can try to iterate through a list of the mapper objects (in the form of
# Python dictionary).
# Based on aggregate key "vin_number" and key "I",
# we will use the value of make to update values w/ key == "A" and "R".

# aggregate_kv_list = []

# for mapper in mapper_list:
#     # Based on aggregate key, we want to set the "make" and "year" values equal to
#     # the same mapper based on "vin_number" column.
#     if mapper["make"] and mapper["year"] is not None:
#         aggregate_make = mapper["make"]
#         aggregate_year = mapper["year"]
#         aggregate_key_value = mapper["vin_number"]
#         # New dict
#         # aggregated_dict = {aggregate_key_value: aggregate_make, aggregate_year}
#         # aggregate_kv_list.append(aggregated_dict)
#
#     if mapper["vin_number"] == aggregate_key_value and (mapper["incident_type"] == "I" or mapper["incident_type"] == "R"):
#         # Store values for year and make based on aggregate key for incident type I and R
#         mapper["year"] = aggregate_year
#         mapper["make"] = aggregate_make


# if mapper["vin_number"] == aggregate_key_value and (mapper["incident_type"] == "I" or mapper["incident_type"] == "R"):
#     # Store values for year and make based on aggregate key for incident type I and R
#     mapper["year"] = aggregate_year
#     mapper["make"] = aggregate_make


# Note:
# - Given the "cat data.csv" command in terminal, we will use the
# CSV file downloaded to perform data parsing, mapping and reducing
# by using "pipe" operator in terminal.

# Stuck:
# - Trying to propagate values based on vin_number.







