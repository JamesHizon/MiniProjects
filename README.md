# MiniProjects
This repository will host remaining Mini-projects through Springboard.

### Data Pipeline Mini-Project

For the Data Pipeline Mini-Project, I am creating a simple data pipeline using Python and MySQL to
get the most popular ticket in the past month.
In order to run the Python script, ensure you are using Python Version 3 and check the requirements.txt file inside the Data_Pipeline_Mini_Project folder to observe which packages need to be installed.

In addition, after cloning this repository, make sure you are in the proper file folder to run
the Python script. Run the following inside terminal.

```
cd Data_Pipeline_Mini_Project
python3 Ticket_system.py
```

The CSV file input is in the following format:

![Screenshot](https://github.com/JamesHizon/MiniProjects/blob/master/Data_Pipeline_Mini_Project/Screen%20Shot%202021-05-06%20at%208.59.13%20AM.png)


The following output should be returned inside Terminal:

```
(venv) (base) jameshizon@Jamess-MBP Data_Pipeline_Mini_Project % python3 Ticket_system.py     
[('Christmas Spectacular', Decimal('5'))]
```
