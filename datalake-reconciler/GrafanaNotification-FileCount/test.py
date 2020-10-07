list1 = ["10", "15", "20", "25", "30", "35", "40"]
list2 = ["25", "40", "35"] 

list3 = list(set(list1) - set(list2))
list4 = list(set(list2) - set(list1))
print("list3 = ", list3)
print("list4 = ", list4)

lsMissingS3Key = list(set(list1)^set(list2))
print("lsMissingS3Key = ", lsMissingS3Key)