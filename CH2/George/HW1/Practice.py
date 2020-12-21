import numpy as np
import pandas as pd
from collections import deque


# List demo
list_1 = [3,2,1,4,7,6,5,8,9]

print(list_1)
print(type(list_1))
list_1.append(10)
print(list_1)
sorted_list = sorted(list_1)
print(sorted_list)
print(list_1)
list_1.sort()
print(list_1)
list_1.pop()
print(list_1)

# deque demo
double_queue = deque([3,1,2,5])
print(double_queue)
double_queue.append(4)
print(double_queue)
double_queue.appendleft(0)
print(double_queue)
double_queue.pop()
print(double_queue)
double_queue.popleft()
print(double_queue)
print(sorted(double_queue))

# dictionary demo
dictionary = {'Brand': 'Ford', 'Model': 'Focus', 'Year': 2018}
print(dictionary)
print(dictionary['Brand'])
dictionary['Category'] = 'Sedan'
print(dictionary)
dictionary['Colours'] = ['red', 'yellow', 'black', 'white']
print(dictionary)
print(dictionary.keys())
print(dictionary.items())

for (key, item) in dictionary.items():
    print('The key is: ', key, ' , and item is: ', item)


# tree demo
class tree:

    def __init__(self, data):

        self.left = None
        self.right = None
        self.data = data

    def PrintTree(self):
        if self.left:
            self.left.PrintTree()
        print(self.data),
        if self.right:
            self.right.PrintTree()

    def insert(self, data):
        # compare current value with parent node
        if self.data:
            if data < self.data:
                if self.left is None:
                    self.left = tree(data)
                else:
                    self.left.insert(data)
            elif data > self.data:
                if self.right is None:
                    self.right = tree(data)
                else:
                    self.right.insert(data)
        else:
            self.data = data

    # dfs algorithm
    # inorder treversal
    # Left -> root -> Right
    def inorder(self,root):
        res=[]
        if root:
            res = self.inorder(root.left)
            res.append(root.data)
            res = res + self.inorder(root.right)
        return res
    # preorder Treversal
    # Root -> Left -> Right
    def preorder(self, root):
        res = []
        if root:
            res.append(root.data)
            res = res + self.preorder(root.left)
            res = res + self.preorder(root.right)
        return res
    # Postorder Treverse
    # Left _> Right -> Root
    def postorder(self, root):
        res = []
        if root:
            res = self.postorder(root.left)
            res = res + self.postorder(root.right)
            res.append(root.data)
        return res

#     bfs to be continued
#     def bfs(self,visited, node):




root = tree(12)
root.insert(6)
root.insert(14)
root.insert(3)
root.insert(35)
root.insert(10)
root.insert(19)
root.insert(31)
root.insert(42)

# root.PrintTree()
print(root.inorder(root))
print(root.preorder(root))
print(root.postorder(root))