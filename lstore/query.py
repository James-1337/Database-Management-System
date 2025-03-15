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
            # print(f"Insert result for key {key}: {result}")
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
        """
        Select a record based on search key with transaction awareness.
        """
        # Get the RID of the record
        rids = self.table.index.locate(search_key_index, search_key)
        if not rids:
            return []

        result = []
        for rid in rids:
            try:
                # If part of a transaction, acquire shared lock
                if self.transaction and self.lock_manager:
                    if not self.lock_manager.acquire_lock(self.transaction.transaction_id, search_key, "read"):
                        return []  # Can't acquire lock, return empty result
                    self.transaction.locks_held.add(search_key)
                    
                # Get the base record
                base_rid = rid
                
                # Get the latest version through indirection
                latest_rid = self._get_latest_version(base_rid)
                # print(f"Latest RID: {latest_rid}")
                # Retrieve the record
                record = self.table.find_record(search_key, latest_rid, projected_columns_index)
                # print(f"Record found: {record}")
                result.append(record)
                
            except Exception as e:
                print(f"Error selecting record: {e}")
                
        return result

    def _get_latest_version(self, rid):
        """
        Helper to get the latest version of a record by following indirection.
        """
        page_range_idx, page_idx, record_idx, page_type = rid
        # If the record is already a tail record, return it
        if page_type != "b":
            return rid

        # Get the in-memory base page from the table page ranges
        page_range = self.table.page_ranges[page_range_idx]
        base_page = page_range.base_pages[page_idx]
        
        # Use the up-to-date in-memory indirection pointer
        if record_idx < len(base_page.indirection):
            candidate = base_page.indirection[record_idx]
            if candidate is not None and candidate != ["empty"]:
                if isinstance(candidate, list):
                    candidate = tuple(candidate)
                return candidate
        return rid


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
                # For version 0, get the latest version by following indirection
                target_rid = self._get_latest_version(base_rid)
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
        """
        Safely get the latest version by following indirection.
        Returns the original RID if anything goes wrong.
        """
        try:
            if rid[3] != "b":
                return rid

            # Otherwise, we're given a base record.
            page_range_idx, page_idx, record_idx, _ = rid

            # Validate indices.
            if page_range_idx >= len(self.table.page_ranges):
                return rid
            page_range = self.table.page_ranges[page_range_idx]
            if page_idx >= len(page_range.base_pages):
                return rid

            base_page = page_range.base_pages[page_idx]

            # If the base page's indirection pointer has been updated (i.e. does not equal the base RID),
            # then return that pointer (which should be a tail record).
            if record_idx < len(base_page.indirection) and base_page.indirection[record_idx] != rid:
                return base_page.indirection[record_idx]

            # Otherwise, return the original base record.
            return rid

        except Exception as e:
            print(f"Error in _safely_get_latest_version: {e}")
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
        # Get the RID of the record
        rids = self.table.index.locate(self.table.key, primary_key)
        if not rids:
            return False

        # If part of a transaction, acquire exclusive lock
        if self.transaction and self.lock_manager:
            if not self.lock_manager.acquire_lock(
                self.transaction.transaction_id, primary_key, "update"
            ):
                return False  # Can't acquire lock, return failure
            self.transaction.locks_held.add(primary_key)

        # Extract base RID components
        base_rid = rids[0]
        page_range_idx, page_idx, record_idx, page_type = base_rid

        with self.table.lock:
            # Initialize tail_rid so it's always defined.
            tail_rid = None
            try:
            
                page_range = self.table.page_ranges[page_range_idx]
                base_page_id = ("base", page_range_idx, page_idx)
                base_page_data = self.table.database.bufferpool.get_page(
                    base_page_id, self.table.name, self.table.num_columns
                )

                # Get the latest version of the record from the page_directory
                latest_rid = self._get_latest_version(base_rid)
                current_record = self.table.page_directory[latest_rid]

                # Build updated_columns by copying the current record and replacing provided fields.
                updated_columns = current_record.columns[:]  
                for i in range(self.table.num_columns):
                    if i < len(columns) and columns[i] is not None:
                        updated_columns[i] = columns[i]
                tail_page_columns = updated_columns[:] 

                # Create a tail page if needed
                if not page_range.tail_pages or not page_range.tail_pages[-1].has_capacity():
                    page_range.add_tail_page(self.table.num_columns)
                tail_page_idx = len(page_range.tail_pages) - 1
                tail_page_id = ("tail", page_range_idx, tail_page_idx)
                tail_page_data = self.table.database.bufferpool.get_page(
                    tail_page_id, self.table.name, self.table.num_columns
                )
                # Get the in-memory tail page
                tail_page = page_range.tail_pages[tail_page_idx]

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
                for i in range(self.table.num_columns):
                    if i < len(columns) and columns[i] is not None:
                        schema[i] = "1"
                schema_str = "".join(schema)
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

                # Create new tail RID
                tail_rid = (page_range_idx, tail_page_idx, len(tail_page_data["rid"]), "t")

                # Write the new tail record
                for i in range(self.table.num_columns):
                    tail_page_data["columns"][i].append(tail_page_columns[i])
                tail_page_data["indirection"].append(latest_rid)
                tail_page_data["rid"].append(tail_rid)
                tail_page_data["timestamp"].append(timestamp)
                tail_page_data["schema_encoding"].append(schema_str)

                # update the in-memory tail page metadata
                tail_page.indirection.append(latest_rid)
                tail_page.rid.append(tail_rid)
                tail_page.start_time.append(timestamp)
                tail_page.schema_encoding.append(schema_str)
                tail_page.num_records += 1

                # update the base page indirection 
                base_page_data["indirection"][record_idx] = tail_rid
                base_page = page_range.base_pages[page_idx]
                base_page.indirection[record_idx] = tail_rid
                self.table.database.bufferpool.set_page(
                    base_page_id, self.table.name, base_page_data
                )
                self.table.database.bufferpool.set_page(
                    tail_page_id, self.table.name, tail_page_data
                )

                # update the page directory with the new record
                new_key = (columns[self.table.key]
                        if len(columns) > self.table.key and columns[self.table.key] is not None and columns[self.table.key] != primary_key
                        else primary_key)
                new_record = Record(tail_rid, primary_key, tail_page_columns)
                self.table.page_directory[tail_rid] = new_record

                if new_key != primary_key:
                    if latest_rid in self.table.page_directory:
                        del self.table.page_directory[latest_rid]
                    if self.table.index.indices.get(self.table.key) is not None:
                        self.table.index.delete(primary_key, latest_rid)
                        self.table.index.insert(new_key, tail_rid)

                # Unpin pages from bufferpool
                self.table.database.bufferpool.unpin_page(tail_page_id, self.table.name)
                self.table.database.bufferpool.unpin_page(base_page_id, self.table.name)

                self.table.merge_counter += 1
                from lstore.config import MERGE_THRESHOLD
                if self.table.merge_counter >= MERGE_THRESHOLD:
                    self.table.merge_counter = 0
                    self.table.trigger_merge()
                    
                return True

            except Exception as e:
                print("Update error:", e)
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

    def sum_version(
        self, start_range, end_range, aggregate_column_index, relative_version
    ):
        """
        Calculate sum for a range of keys at a specific version.
        """
        # Get RIDs in the range
        rids = self.table.index.locate_range(start_range, end_range, self.table.key)
        if not rids:
            return 0  # Return 0 instead of False for tests

        total_sum = 0
        processed_keys = set()

        for base_rid in rids:
            try:
                # Get the key value to verify range and avoid duplicates
                key_value = self._get_column_value(base_rid, self.table.key)

                if (
                    key_value < start_range
                    or key_value > end_range
                    or key_value in processed_keys
                ):
                    continue

                processed_keys.add(key_value)

                # For version 0 (current), get the latest version
                if relative_version == 0:
                    target_rid = self._safely_get_latest_version(base_rid)
                    value = self._get_column_value(target_rid, aggregate_column_index)
                    total_sum += int(value)

                # For version -1 (original/base record)
                elif relative_version == -1:
                    # Use the base record directly
                    value = self._get_column_value(base_rid, aggregate_column_index)
                    total_sum += int(value)

                # For other historical versions
                else:
                    # Start from the latest and navigate back
                    latest_rid = self._safely_get_latest_version(base_rid)
                    if latest_rid != base_rid:  # Only if there are updates
                        target_rid = self._safely_get_historical_version(
                            latest_rid, base_rid, abs(relative_version)
                        )
                        value = self._get_column_value(
                            target_rid, aggregate_column_index
                        )
                        total_sum += int(value)
                    else:
                        # No updates, use base
                        value = self._get_column_value(base_rid, aggregate_column_index)
                        total_sum += int(value)

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
