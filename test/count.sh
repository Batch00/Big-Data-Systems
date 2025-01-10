#!/usr/bin/bash
# download the zip file
wget https://pages.cs.wisc.edu/~harter/cs544/data/hdma-wi-2021.zip
# extract the contents of zip file
unzip hdma-wi-2021.zip 
# remove the empty zip file
rm hdma-wi-2021.zip
# print how many lines contain the text "Multifamily"
grep "Multifamily" hdma-wi-2021.csv | wc -l
# remove the scv file because it is very large
rm hdma-wi-2021.csv