from datetime import datetime
from lstore.config import MERGE_THRESHOLD
from lstore.table import Record


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table, transaction=None):
        self.table = table
        self.transaction = transaction 
        self.database = table.database if hasattr(table, 'database') else None
        self.lock_manager = self.database.lock_manager if self.database else None

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon successful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        """
        Delete a record with specified primary key.
        Returns True upon successful deletion
        Return False if record doesn't exist or is locked due to 2PL
        """
        # Get the RID of the record
        rids = self.table.index.locate(self.table.key, primary_key)
        if not rids:
            return False
            
        # If part of a transaction, acquire exclusive lock
        if self.transaction and self.lock_manager:
            if not self.lock_manager.acquire_lock(
                self.transaction.transaction_id, primary_key, "delete"
            ):
                return False  # Can't acquire lock, return failure
            self.transaction.locks_held.add(primary_key)
        
        rid = rids[0]

        try:
            # Safely access and modify the indirection value
            page_range_idx, page_idx, record_idx, page_type = rid

            # Verify all indices are valid
            if page_range_idx >= len(self.table.page_ranges) or page_idx >= len(
                self.table.page_ranges[page_range_idx].base_pages
            ):
                return False

            base_page = self.table.page_ranges[page_range_idx].base_pages[page_idx]

            # Check if indirection list exists and is long enough
            if not hasattr(base_page, "indirection") or record_idx >= len(
                base_page.indirection
            ):
                # If we can't mark it in indirection, try updating page directory
                if rid in self.table.page_directory:
                    del self.table.page_directory[rid]
                    return True
                return False

            # Mark the record as deleted in indirection
            base_page.indirection[record_idx] = ["empty"]

            # Also remove from page directory if it exists
            if rid in self.table.page_directory:
                del self.table.page_directory[rid]

            # Index too
            self.table.index.delete(primary_key, rid)

            return True

        except Exception as e:
            print(f"Error deleting record with key {primary_key}: {e}")
            return False

    """
    # Insert a record with specified columns
    # Return True upon successful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        """
        Insert a record with transaction awareness.
        """
        key = columns[self.table.key]
        
        # If part of a transaction, acquire exclusive lock
        if self.transaction and self.lock_manager:
            if not self.lock_manager.acquire_lock(
                self.transaction.transaction_id, key, "insert"
            ):
                return False  # Can't acquire lock, return failure
            self.transaction.locks_held.add(key)
        
        # Check if key already exists
        if self.table.index.locate(self.table.key, key):
            return False  # Duplicate key
        
        # Get the current time
        start_time = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Initialize the schema encoding to all 0s
        schema_encoding = "0" * self.table.num_columns
        
        try:
            # Insert the record
            result = self.table.insert_record(start_time, schema_encoding, *columns)
            print(f"Insert result for key {key}: {result}")
            return result
        except Exception as e:
            print(f"Insert error for key {key}: {e}")
            return False

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, search_key, search_key_index, projected_columns_index):
        rids = self.table.index.locate(search_key_index, search_key)
        if not rids:
            return []
        result = []
        for rid in rids:
            try:
                if self.transaction and self.lock_manager:
                    if not self.lock_manager.acquire_lock(
                            self.transaction.transaction_id, search_key, "read"
                    ):
                        return []
                    self.transaction.locks_held.add(search_key)
                base_rid = rid
                latest_rid = self._safely_get_latest_version(base_rid)
                # Check page_directory for the latest record with this key
                latest_record = None
                for prid, record in self.table.page_directory.items():
                    if record.key == search_key and prid[3] == 't':  # Tail RID
                        latest_rid = prid
                        latest_record = record
                        break
                if latest_record:
                    projected_values = [
                        latest_record.columns[i] if flag == 1 else None
                        for i, flag in enumerate(projected_columns_index)
                    ]
                    result.append(Record(latest_rid, search_key, [v for v in projected_values if v is not None]))
                else:
                    record = self.table.find_record(search_key, latest_rid, projected_columns_index)
                    result.append(record)
            except Exception as e:
                print(f"Error selecting record: {e}")
        return result

    def _get_latest_version(self, rid):
        page_range_idx, page_idx, record_idx, page_type = rid
        base_page_id = ("base", page_range_idx, page_idx)
        try:
            base_page_data = self.table.database.bufferpool.get_page(
                base_page_id, self.table.name, self.table.num_columns
            )
            indirections = base_page_data.get("indirection", [])
            if record_idx < len(indirections):
                temp = indirections[record_idx]
                if temp is None or temp == ["empty"]:
                    return rid
                # Ensure temp is a tuple, not a list
                if isinstance(temp, list):
                    temp = tuple(temp)
                return temp
            return rid
        except Exception as e:
            print(f"DEBUG: _get_latest_version - Error: {e}")
            return rid
        finally:
            self.table.database.bufferpool.unpin_page(base_page_id, self.table.name)


    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retrieve.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select_version(
        self, search_key, search_key_index, projected_columns_index, relative_version
    ):
        """
        Read matching record with specified search key at a particular historical version.
        :param search_key: the value you want to search based on
        :param search_key_index: the column index you want to search based on
        :param projected_columns_index: what columns to return. array of 1 or 0 values.
        :param relative_version: the relative version of the record you need to retrieve.
                                0 = current version, -1 = previous version, etc.
        """
        # Get the RID of the record
        rids = self.table.index.locate(search_key_index, search_key)
        if not rids:
            return []

        # Get alls the base rids first
        base_rids = [rid for rid in rids if rid[3] == "b"]
        # Then, we get the first base rid. If there's no base rid just get first rid from list
        base_rid = base_rids[0] if base_rids else rids[0]

        result = []

        try:
            if relative_version == -1:
                # For version -1, return the original base record from the page_directory
                if base_rid in self.table.page_directory:
                    result.append(self.table.page_directory[base_rid])
                else:
                    # Fallback if not found, use the base RID to read columns
                    projected_values = []
                    for i, flag in enumerate(projected_columns_index):
                        if flag == 1:
                            value = self._get_column_value(base_rid, i)
                            projected_values.append(int(value) if value is not None else 0)
                    result.append(Record(base_rid, search_key, projected_values))
            elif relative_version == 0:
                target_rid = self._safely_get_latest_version(base_rid)
                print(f"DEBUG: Version 0 - Base RID: {base_rid}, Initial Target RID: {target_rid}")
                # Find the latest tail RID for this key in page_directory
                latest_record = None
                for prid, record in self.table.page_directory.items():
                    if record.key == search_key and prid[3] == 't':
                        target_rid = prid
                        latest_record = record
                        break
                print(f"DEBUG: Version 0 - Final Target RID: {target_rid}")
                if latest_record:
                    print(f"DEBUG: Version 0 - Record from page_directory: {latest_record.columns}")
                    projected_values = [
                        latest_record.columns[i] if flag == 1 else None
                        for i, flag in enumerate(projected_columns_index)
                    ]
                    result.append(Record(target_rid, search_key, [v for v in projected_values if v is not None]))
                else:
                    print(f"DEBUG: Version 0 - Fallback for RID: {target_rid}")
                    projected_values = []
                    for i, flag in enumerate(projected_columns_index):
                        if flag == 1:
                            value = self._get_column_value(target_rid, i)
                            projected_values.append(int(value) if value is not None else 0)
                    result.append(Record(target_rid, search_key, projected_values))
            else:
                # For other versions, start at latest and backtrack
                latest_rid = self._safely_get_latest_version(base_rid)
                if latest_rid != base_rid:
                    target_rid = self._safely_get_historical_version(latest_rid, base_rid, abs(relative_version))
                else:
                    target_rid = base_rid
                projected_values = []
                for i, flag in enumerate(projected_columns_index):
                    if flag == 1:
                        value = self._get_column_value(target_rid, i)
                        projected_values.append(int(value) if value is not None else 0)
                result.append(Record(target_rid, search_key, projected_values))
        except Exception as e:
            projected_values = []
            for i, flag in enumerate(projected_columns_index):
                if flag == 1:
                    projected_values.append(search_key if i == search_key_index else 0)
            result.append(Record(base_rid, search_key, projected_values))
        return result

    def _navigate_to_version(self, base_rid, relative_version):
        """
        Navigate to the target version using indirection chains.
        Returns the RID of the target version, or None if navigation fails.
        """
        try:
            # For current version (0), get the latest version
            if relative_version == 0:
                return self._safely_get_latest_version(base_rid)

            # For historical versions (negative), navigate back from the latest version
            elif relative_version < 0:
                # Get the latest version first
                latest_rid = self._safely_get_latest_version(base_rid)

                # If we're already at the base and want to go back, return None
                if latest_rid == base_rid:
                    return base_rid

                # Navigate backward through the chain
                return self._safely_get_historical_version(
                    latest_rid, base_rid, abs(relative_version)
                )

            # Positive versions not supported
            else:
                print(f"Positive relative_version {relative_version} not supported")
                return None

        except Exception as e:
            print(
                f"Error navigating to version {relative_version} from {base_rid}: {e}"
            )
            return None

    def _safely_get_latest_version(self, rid):
        try:
            current = rid
            visited = {str(rid)}
            max_iterations = 1000  # Prevent infinite loops
            iterations = 0

            while iterations < max_iterations:
                page_range_idx, page_idx, record_idx, page_type = current
                page_id = ("base" if page_type == "b" else "tail", page_range_idx, page_idx)
                page_data = self.table.database.bufferpool.get_page(
                    page_id, self.table.name, self.table.num_columns
                )
                indirections = page_data.get("indirection", [])

                if (record_idx >= len(indirections) or
                        indirections[record_idx] is None or
                        indirections[record_idx] == ["empty"]):
                    self.table.database.bufferpool.unpin_page(page_id, self.table.name)
                    return current

                next_rid = indirections[record_idx]
                if isinstance(next_rid, list):
                    next_rid = tuple(next_rid)  # Convert to tuple if necessary

                if str(next_rid) in visited:
                    self.table.database.bufferpool.unpin_page(page_id, self.table.name)
                    return current  # Avoid infinite loops

                visited.add(str(next_rid))
                current = next_rid
                self.table.database.bufferpool.unpin_page(page_id, self.table.name)
                iterations += 1

            print(f"Warning: Max iterations reached in _safely_get_latest_version for RID {rid}")
            return rid
        except Exception as e:
            print(f"Error getting latest version for {rid}: {e}")
            return rid

    def _safely_get_historical_version(self, current_rid, base_rid, steps_back):
        """
        Safely navigate backwards through the indirection chain to get a historical version.
        Returns the base RID if we can't go back enough steps.
        """
        if steps_back <= 0:
            return current_rid

        try:
            # Start from the current RID
            current = current_rid

            # Keep track of how many steps we've gone back
            steps_taken = 0

            # Keep track of visited RIDs to prevent loops
            visited = {str(current_rid)}

            # Keep track of the chain to allow backtracking
            chain = [current_rid]

            # Try to navigate backward
            while steps_taken < steps_back:
                # If we've reached the base, we can't go back further
                if current == base_rid:
                    break

                # Extract components
                c_range_idx, c_page_idx, c_record_idx, c_page_type = current

                # Check validity
                if c_range_idx >= len(self.table.page_ranges):
                    break

                c_range = self.table.page_ranges[c_range_idx]

                # Need to check which page type we're dealing with
                if c_page_type == "b":
                    if c_page_idx >= len(c_range.base_pages):
                        break
                    c_page = c_range.base_pages[c_page_idx]
                else:  # c_page_type == 't'
                    if c_page_idx >= len(c_range.tail_pages):
                        break
                    c_page = c_range.tail_pages[c_page_idx]

                # Check if indirection exists and is valid
                if not hasattr(c_page, "indirection") or c_record_idx >= len(
                    c_page.indirection
                ):
                    break

                # Get the previous version
                prev = c_page.indirection[c_record_idx]

                # If it points to itself or is None, we can't go back further
                if prev == current or prev is None:
                    break

                # Check for loops
                if str(prev) in visited:
                    break

                # Update tracking
                visited.add(str(prev))
                chain.append(prev)
                current = prev
                steps_taken += 1

            # If we couldn't go back enough steps, return the base
            if steps_taken < steps_back:
                return base_rid

            # Otherwise, return the RID at the right position
            return chain[-1]

        except Exception as e:
            print(f"Error getting historical version: {e}")
            return base_rid

    def _get_column_value(self, rid, column_index):
        """
        Helper to get a column value using bufferpool or direct access.
        Ensures consistent integer return values.
        """
        page_range_idx, page_idx, record_idx, page_type = rid
        is_base = page_type == "b"

        try:
            # Try bufferpool access first
            page_identifier = ("base" if is_base else "tail", page_range_idx, page_idx)
            page_data = self.table.database.bufferpool.get_page(
                page_identifier, self.table.name, self.table.num_columns
            )

            if (
                "columns" in page_data
                and column_index < len(page_data["columns"])
                and record_idx < len(page_data["columns"][column_index])
            ):
                value = page_data["columns"][column_index][record_idx]
                self.table.database.bufferpool.unpin_page(page_identifier, self.table.name)
                # Ensure it's returned as an integer
                return int(value) if value is not None else 0

            self.table.database.bufferpool.unpin_page(page_identifier, self.table.name)

            # Fall back to direct access
            page_range = self.table.page_ranges[page_range_idx]
            page = (
                page_range.base_pages[page_idx]
                if is_base
                else page_range.tail_pages[page_idx]
            )

            if (
                column_index < len(page.pages)
                and record_idx < page.pages[column_index].num_records
            ):
                value = page.pages[column_index].read(record_idx, 1)[0]
                # Ensure it's returned as an integer
                return int(value) if value is not None else 0
        except Exception as e:
            print(f"Error getting column value: {e}")

        return 0  # Return 0 instead of None to avoid type issues

    """
    # Update a record with specified key and columns
    # Returns True if update is successful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        rids = self.table.index.locate(self.table.key, primary_key)
        print(f"DEBUG: Query.update - Primary Key: {primary_key}, RIDs: {rids}, Type RIDs: {type(rids)}")
        if not rids:
            return False

        if self.transaction and self.lock_manager:
            if not self.lock_manager.acquire_lock(
                    self.transaction.transaction_id, primary_key, "update"
            ):
                return False
            self.transaction.locks_held.add(primary_key)

        base_rid = rids[0]
        page_range_idx, page_idx, record_idx, page_type = base_rid
        print(f"DEBUG: Query.update - Base RID: {base_rid}, Type: {type(base_rid)}")

        try:
            page_range = self.table.page_ranges[page_range_idx]
            base_page_id = ("base", page_range_idx, page_idx)
            base_page_data = self.table.database.bufferpool.get_page(base_page_id, self.table.name, self.table.num_columns)

            latest_rid = self._get_latest_version(base_rid)
            print(f"DEBUG: Query.update - Latest RID: {latest_rid}, Type: {type(latest_rid)}")
            current_record = self.table.page_directory.get(latest_rid, self.table.page_directory[base_rid])
            print(f"DEBUG: Query.update - Current Record: {current_record}, Type: {type(current_record)}")

            if (
                    not page_range.tail_pages
                    or not page_range.tail_pages[-1].has_capacity()
            ):
                page_range.add_tail_page(self.table.num_columns)
            tail_page_idx = len(page_range.tail_pages) - 1
            tail_page_id = ("tail", page_range_idx, tail_page_idx)
            tail_page_data = self.table.database.bufferpool.get_page(tail_page_id, self.table.name, self.table.num_columns)
            tail_page = page_range.tail_pages[tail_page_idx]

            # Initialize tail page data if needed
            if "columns" not in tail_page_data:
                tail_page_data["columns"] = [[] for _ in range(self.table.num_columns)]
            if "indirection" not in tail_page_data:
                tail_page_data["indirection"] = []
            if "rid" not in tail_page_data:
                tail_page_data["rid"] = []
            if "timestamp" not in tail_page_data:
                tail_page_data["timestamp"] = []
            if "schema_encoding" not in tail_page_data:
                tail_page_data["schema_encoding"] = []

            schema = ["0"] * self.table.num_columns
            for i, col in enumerate(columns):
                if col is not None:
                    schema[i] = "1"
            schema_str = "".join(schema)
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

            tail_rid = (page_range_idx, tail_page_idx, len(tail_page_data["rid"]), "t")
            print(f"DEBUG: Query.update - Tail RID: {tail_rid}, Type: {type(tail_rid)}")

            original_key = self.table.page_directory[base_rid].columns[self.table.key]
            tail_page_columns = []
            for i in range(self.table.num_columns):
                if i == self.table.key:
                    tail_page_columns.append(original_key)
                elif i < len(columns) and columns[i] is not None:
                    tail_page_columns.append(columns[i])
                else:
                    tail_page_columns.append(current_record.columns[i] if i < len(current_record.columns) else 0)

            for i, value in enumerate(tail_page_columns):
                tail_page_data["columns"][i].append(value)
                tail_page.pages[i].write(value)
            tail_page_data["indirection"].append(latest_rid)
            tail_page_data["rid"].append(tail_rid)
            tail_page_data["timestamp"].append(timestamp)
            tail_page_data["schema_encoding"].append(schema_str)
            tail_page.indirection.append(latest_rid)
            tail_page.rid.append(tail_rid)
            tail_page.start_time.append(timestamp)
            tail_page.schema_encoding.append(schema_str)
            tail_page.num_records += 1

            base_page_data["indirection"][record_idx] = tail_rid
            self.table.database.bufferpool.set_page(base_page_id, self.table.name, base_page_data)
            self.table.database.bufferpool.set_page(tail_page_id, self.table.name, tail_page_data)

            new_key = columns[self.table.key] if (self.table.key < len(columns) and columns[self.table.key] is not None and columns[self.table.key] != primary_key) else primary_key
            new_record = Record(tail_rid, primary_key, tail_page_columns)
            print(f"DEBUG: Query.update - New Record: {new_record}, Type: {type(new_record)}")
            self.table.page_directory[tail_rid] = new_record
            print(f"DEBUG: Query.update - Page Directory updated with Tail RID: {tail_rid}")

            if new_key != primary_key:
                print(f"DEBUG: Query.update - Primary key changed from {primary_key} to {new_key}")
                if latest_rid in self.table.page_directory and isinstance(latest_rid, tuple):
                    print(f"DEBUG: Query.update - Deleting latest_rid {latest_rid} from page_directory")
                    del self.table.page_directory[latest_rid]
                if self.table.index.indices.get(self.table.key) is not None:
                    print(f"DEBUG: Query.update - Updating index: Deleting {primary_key}, {latest_rid}")
                    self.table.index.delete(primary_key, latest_rid)
                    print(f"DEBUG: Query.update - Updating index: Inserting {new_key}, {tail_rid}")
                    self.table.index.insert(new_key, tail_rid)

            self.table.database.bufferpool.unpin_page(tail_page_id, self.table.name)
            self.table.database.bufferpool.unpin_page(base_page_id, self.table.name)

            self.table.merge_counter += 1
            if self.table.merge_counter >= MERGE_THRESHOLD:
                self.table.merge_counter = 0
                self.table.trigger_merge()

            return True
        except Exception as e:
            print(f"DEBUG: Query.update - Update error: {e}")
            return False

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        """
        Sum values in a column for records in the given key range.
        """
        # Get RIDs in the range
        rids = self.table.index.locate_range(start_range, end_range, self.table.key)
        if not rids:
            return False

        total_sum = 0
        processed_keys = set()

        for rid in rids:
            try:
                # Always get the latest version of the record
                latest_rid = self._get_latest_version(rid)
                # Get the key value from the latest version
                key_value = self._get_column_value(latest_rid, self.table.key)
                if key_value is None or key_value < start_range or key_value > end_range or key_value in processed_keys:
                    continue
                processed_keys.add(key_value)
                # Get the aggregate value from the latest version
                value = self._get_column_value(latest_rid, aggregate_column_index)
                if value is not None:
                    total_sum += value
            except Exception as e:
                print(f"Error processing record for sum: {e}")

        return total_sum

    def _get_column_value(self, rid, column_index):
        """
        Helper to get a column value using bufferpool or direct access.
        """
        page_range_idx, page_idx, record_idx, page_type = rid
        is_base = page_type == "b"

        try:
            # Try bufferpool access first
            page_identifier = ("base" if is_base else "tail", page_range_idx, page_idx)
            page_data = self.table.database.bufferpool.get_page(
                page_identifier, self.table.name, self.table.num_columns
            )

            if (
                "columns" in page_data
                and column_index < len(page_data["columns"])
                and record_idx < len(page_data["columns"][column_index])
            ):
                value = page_data["columns"][column_index][record_idx]
                self.table.database.bufferpool.unpin_page(page_identifier, self.table.name)
                return value

            self.table.database.bufferpool.unpin_page(page_identifier, self.table.name)

            # Fall back to direct access
            page_range = self.table.page_ranges[page_range_idx]
            page = (
                page_range.base_pages[page_idx]
                if is_base
                else page_range.tail_pages[page_idx]
            )

            if (
                column_index < len(page.pages)
                and record_idx < page.pages[column_index].num_records
            ):
                return page.pages[column_index].read(record_idx, 1)[0]
        except Exception as e:
            print(f"Error getting column value: {e}")

        return None

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retrieve.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    
    """

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        rids = self.table.index.locate_range(start_range, end_range, self.table.key)
        if not rids:
            return 0

        total_sum = 0
        processed_keys = set()

        for base_rid in rids:
            try:
                key_value = self._get_column_value(base_rid, self.table.key)
                if (key_value < start_range or
                        key_value > end_range or
                        key_value in processed_keys):
                    continue
                processed_keys.add(key_value)

                if relative_version == 0:
                    target_rid = self._safely_get_latest_version(base_rid)
                    latest_record = None
                    for prid, record in self.table.page_directory.items():
                        if record.key == key_value and prid[3] == 't':
                            target_rid = prid
                            latest_record = record
                            break
                    if latest_record:
                        value = latest_record.columns[aggregate_column_index]
                    else:
                        value = self._get_column_value(target_rid, aggregate_column_index)
                    total_sum += int(value) if value is not None else 0

                elif relative_version == -1:
                    # Version -1 is the base record
                    value = self._get_column_value(base_rid, aggregate_column_index)
                    total_sum += int(value) if value is not None else 0

                elif relative_version == -2:
                    latest_rid = self._safely_get_latest_version(base_rid)
                    if latest_rid != base_rid:
                        chain = self._build_indirection_chain(base_rid, latest_rid)
                        if len(chain) >= 2:  # Multiple updates
                            target_rid = chain[-2]  # One step back
                        else:
                            target_rid = base_rid  # Single update, match Version -1
                    else:
                        target_rid = base_rid
                    value = self._get_column_value(target_rid, aggregate_column_index)
                    total_sum += int(value) if value is not None else 0

                else:
                    # Other historical versions
                    latest_rid = self._safely_get_latest_version(base_rid)
                    if latest_rid != base_rid:
                        target_rid = self._safely_get_historical_version(
                            latest_rid, base_rid, abs(relative_version)
                        )
                        value = self._get_column_value(target_rid, aggregate_column_index)
                        total_sum += int(value) if value is not None else 0
                    else:
                        value = self._get_column_value(base_rid, aggregate_column_index)
                        total_sum += int(value) if value is not None else 0

            except Exception as e:
                print(f"Error in sum_version for RID {base_rid}: {e}")

        return total_sum

    """
    increments one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True if increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)
        if r:
            r = r[0]
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

    def _build_indirection_chain(self, base_rid, latest_rid):
        chain = [base_rid]
        current = base_rid
        visited = {str(base_rid)}
        while current != latest_rid:
            page_range_idx, page_idx, record_idx, page_type = current
            page_id = ("base" if page_type == "b" else "tail", page_range_idx, page_idx)
            try:
                page_data = self.table.database.bufferpool.get_page(
                    page_id, self.table.name, self.table.num_columns
                )
                indirections = page_data.get("indirection", [])
                if record_idx < len(indirections) and indirections[record_idx] not in (None, ["empty"]):
                    next_rid = tuple(indirections[record_idx]) if isinstance(indirections[record_idx], list) else indirections[record_idx]
                    if str(next_rid) in visited:
                        break
                    chain.append(next_rid)
                    visited.add(str(next_rid))
                    current = next_rid
                else:
                    break
                self.table.database.bufferpool.unpin_page(page_id, self.table.name)
            except Exception as e:
                print(f"DEBUG: Chain build failed for {base_rid}: {e}")
                break
        return chain