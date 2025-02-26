# B Plus Tree Implementation
# Internal nodes store keys while leaf nodes store (key, rid) pairs
class BPlusTreeNode:
    def __init__(self, leaf=False):
        self.leaf = leaf
        self.keys = []
        self.children = []
        self.next = None
        self.parent = None

class BPlusTree:
    def __init__(self, t):
        # Start with an empty node, root is the root node and t is the minimum degree of the tree
        self.root = BPlusTreeNode(True)
        self.t = t

    # Leaf finding operation
    def find_leaf(self, key):
        # Go down to a leaf
        node = self.root
        while not node.leaf:
            i = 0
            while (i < len(node.keys)) and (node.keys[i] <= key):
                i += 1
            node = node.children[i]
        return node

    # Search operation
    def search(self, key):
        leaf = self.find_leaf(key)
        result = []
        # Search the leaf to see if the key exists
        # return the rid if the key matches
        for k, r in leaf.keys:
            if k == key:
                result.append(r)
        return result

    # Insertion operation for inserting into leafs
    def insert(self, key, rid):
        # check the leaf to insert into and split it if full
        leaf = self.find_leaf(key)
        i = 0
        while i < len(leaf.keys) and leaf.keys[i][0] < key:
            i += 1
        # Skip if duplicate
        if i < len(leaf.keys) and leaf.keys[i][0] == key:
            return
        leaf.keys.insert(i, (key, rid))
        if len(leaf.keys) > (self.t * 2) - 1:
            self.split_leaf(leaf)

    # Leaf splitting operation for full leafs
    def split_leaf(self, leaf):
        # Split the leaf in half into two different leaves
        new_leaf = BPlusTreeNode(leaf=True)
        new_leaf.parent = leaf.parent
        split = len(leaf.keys) // 2
        new_leaf.keys = leaf.keys[split:]
        leaf.keys = leaf.keys[:split]

        # Update the pointers and keep the structure
        new_leaf.next = leaf.next
        leaf.next = new_leaf
        new_key = new_leaf.keys[0][0]
        self.insert_in(leaf, new_key, new_leaf)

    # Internal node splitting operation
    def split_internal(self, node):
        # Split the internal node into two different nodes and split the children and promote the middle key
        new_internal = BPlusTreeNode(leaf=False)
        new_internal.parent = node.parent
        split = len(node.keys) // 2
        promote_key = node.keys[split]
        new_internal.keys = node.keys[split + 1:]
        node.keys = node.keys[:split]
        new_internal.children = node.children[split + 1:]
        node.children = node.children[:split + 1]

        # Update the pointers and keep the structure
        for child in new_internal.children:
            child.parent = new_internal

        self.insert_in(node, promote_key, new_internal)

    # Insertion operation for inserting into an internal node
    def insert_in(self, node, key, new_node):
        # If the node is the root
        if node == self.root:
            # Set a new root and add the key into it
            new_root = BPlusTreeNode(leaf=False)
            new_root.keys.append(key)
            new_root.children.append(node)
            new_root.children.append(new_node)
            node.parent = new_root
            new_node.parent = new_root
            self.root = new_root
            return

        # Otherwise just insert into the parent and split the parent if it is full
        i = 0
        parent = node.parent
        while i < len(parent.keys) and key > parent.keys[i]:
            i += 1
        parent.keys.insert(i, key)
        parent.children.insert(i + 1, new_node)
        new_node.parent = parent
        if len(parent.keys) > (2 * self.t) - 1:
            self.split_internal(parent)

    # Traverse operation
    def traverse(self, begin=None, end=None):
        node = self.root
        # Keep a result for returning
        result = []

        # Search a range if a range is specified and start at the leftmost leaf otherwise
        if begin is not None:
            node = self.find_leaf(begin)
        else:
            while not node.leaf:
                node = node.children[0]

        # if the node does not exist, then return
        if node is None:
            return result

        # traverse through the linked leafs from left to right bounds and gather the rids
        while node:
            for key, rid in node.keys:
                if begin is not None and key < begin:
                    continue
                if end is not None and key > end:
                    return result
                result.append(rid)
            node = node.next
        return result

    # Deletion operation
    def delete(self, key):
        leaf = self.find_leaf(key)

        # Binary search to find the key
        left, right = 0, len(leaf.keys)
        while left < right:
            mid = (left + right) // 2
            if leaf.keys[mid][0] < key:
                left = mid + 1
            else:
                right = mid

        # If key exists, remove it
        if left < len(leaf.keys) and leaf.keys[left][0] == key:
            leaf.keys.pop(left)

            # Handle root case
            if leaf == self.root:
                if not leaf.keys:
                    self.root = BPlusTreeNode(leaf=True)
                return

            # Fix underflow if necessary
            if len(leaf.keys) < self.t:
                self.fix_structure(leaf)

    # Restoration function for keeping structure after deletion
    def fix_structure(self, node):
        if node == self.root:
            if not node.leaf and len(node.children) == 1:
                self.root = node.children[0]
                self.root.parent = None
            return

        parent = node.parent
        index = parent.children.index(node)
        if index > 0:
            left_sibling = parent.children[index - 1]
        else:
            left_sibling = None
        if index < len(parent.children) - 1:
            right_sibling = parent.children[index + 1]
        else:
            right_sibling = None

        # Borrow from left sibling if possible
        if left_sibling and len(left_sibling.keys) > self.t:
            if node.leaf:
                borrowed = left_sibling.keys.pop(-1)
                node.keys.insert(0, borrowed)
                parent.keys[index - 1] = node.keys[0][0]
            else:
                borrowed_key = left_sibling.keys.pop(-1)
                borrowed_child = left_sibling.children.pop(-1)
                node.keys.insert(0, parent.keys[index - 1])
                node.children.insert(0, borrowed_child)
                borrowed_child.parent = node
                parent.keys[index - 1] = borrowed_key
            return

        # Borrow from right sibling if possible
        if right_sibling and len(right_sibling.keys) > self.t:
            if node.leaf:
                borrowed = right_sibling.keys.pop(0)
                node.keys.append(borrowed)
                parent.keys[index] = right_sibling.keys[0][0] if right_sibling.keys else None
            else:
                borrowed_key = right_sibling.keys.pop(0)
                borrowed_child = right_sibling.children.pop(0)
                node.keys.append(parent.keys[index])
                node.children.append(borrowed_child)
                borrowed_child.parent = node
                parent.keys[index] = borrowed_key
            return

        # Merge with a sibling (prefer left if available)
        if left_sibling:
            if node.leaf:
                left_sibling.keys.extend(node.keys)
                left_sibling.next = node.next
            else:
                left_sibling.keys.append(parent.keys[index - 1])
                left_sibling.keys.extend(node.keys)
                left_sibling.children.extend(node.children)
                for child in node.children:
                    child.parent = left_sibling
            parent.keys.pop(index - 1)
            parent.children.pop(index)
        elif right_sibling:
            if node.leaf:
                node.keys.extend(right_sibling.keys)
                node.next = right_sibling.next
            else:
                node.keys.append(parent.keys[index])
                node.keys.extend(right_sibling.keys)
                node.children.extend(right_sibling.children)
                for child in right_sibling.children:
                    child.parent = node
            parent.keys.pop(index)
            parent.children.pop(index + 1)
        else:
            return

        # Fix parent if necessary
        if len(parent.keys) < self.t:
            self.fix_structure(parent)

class Index:
    def __init__(self, table, t=3):
        # One index for each table. All are empty initially
        self.table = table
        self.t = t
        self.indices = [None] * table.num_columns

    """
    # returns the location of all records with the given value on column "column"
    """
    def locate(self, column, value):
        # If column is out of range or no index exists for the column, return empty list
        if column >= len(self.indices) or self.indices[column] is None:
            return []
        # Return the list of RIDs for the given value in the column, empty list if value not found
        return self.indices[column].search(value)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column):
        # If column is out of range or no index exists for the column, return empty list
        if column >= len(self.indices) or self.indices[column] is None:
            return []
        # Return the list of RIDs for the given range in the column, empty list if no records found
        return self.indices[column].traverse(begin, end)


    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        # Create a B-Tree
        self.indices[column_number] = BPlusTree(self.t)
        # Create index for the column
        for rid, record in self.table.page_directory.items():
            value = record.columns[column_number]
            self.indices[column_number].insert(value, rid)

    def insert(self, key, rid):
        # If index for the column does not exist, create one
        if self.indices[0] is None:
            self.create_index(0)
        # Insert the key and RID into the index using the B-Tree
        self.indices[0].insert(key, rid)

    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):
        # If column is out of range, do nothing
        if column_number >= len(self.indices):
            return
        # Drop index for the column
        self.indices[column_number] = None

    # Delete a value from the index
    def delete(self, column, value):
        if column < len(self.indices) and self.indices[column] is not None:
            self.indices[column].delete(value)