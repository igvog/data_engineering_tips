a = 100

# +1
# *2
# input = 13
# output = 8
# 0 +1 = 1 
# *2 = 2
# *2 = 4
# +1 = 5
# *2 =10
# +1 +1 +1 

  
#test_list = range(1, a + 1)
#print(test_list)

#+1 *2 +1 *2 *2 +1 

def getMinOperations(kValues):
    # Write your code here
    output = []
    for i in kValues:
        output.append(getMinOperations_rev(i))
    return output


def getMinOperations_rev(k):
    operation_numbers = 0
    while k>0:
        k = k/2 if k%2==0 else k-1
        operation_numbers = operation_numbers + 1
    return operation_numbers

kvalues = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]

print(getMinOperations(kvalues))