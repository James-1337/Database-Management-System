from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query
from lstore.db import LockManager
import os
from threading import RLock

class Transaction:
    def __init__(self, transaction_id=None, buffer_pool=None, lock_manager=None):
        self.transaction_id = transaction_id if transaction_id is not None else id(self)  # Unique ID for transaction
        self.queries = []  # List to store queries and their arguments
        self.rollback_operations = []  # List of tuples to store operations for rollback
        self.buffer_pool = buffer_pool  # Reference to buffer pool
        self.lock_manager = lock_manager  # Reference to lock manager
        self.locks_held = set()  # Set to track locks held by this transaction
        self.mutex = RLock()  # Mutex Lock for thread-safe log writes
        self._deleted_records = {} # Deleted records for rollback

    def add_query(self, query, table, *args):
        with self.mutex:
            # Dynamically fetch buffer pool and lock manager from table if not set
            if self.buffer_pool is None and hasattr(table, 'database') and table.database is not None:
                self.buffer_pool = table.database.bufferpool
            if self.lock_manager is None and hasattr(table, 'database') and table.database is not None:
                self.lock_manager = table.database.lock_manager

            # Store querying changes and its arguments as well as current state for potential rollback
            self.queries.append((query, table, args))
            if query.__name__ == "update":
                original_state = self._get_record_columns(table, args[0])
                if original_state is not None:
                    # Make a copy so that changes do not affect the stored state
                    original_state = list(original_state)
                    self.rollback_operations.append((lambda key, original_state=original_state: Query(table).update(key, *original_state), args))
                else:
                    self.rollback_operations.append((lambda key: None, args))
            # For insert queries, we rollback by deleting the inserted record
            elif query.__name__ == "insert":
                self.rollback_operations.append((lambda key: Query(table).delete(key), args))
            # For delete queries, since we might need to re-insert it we can store it
            elif query.__name__ == "delete":
                self.rollback_operations.append((lambda key: self._restore_deleted_record(table, key), args))

    # Helper function for possible rollback
    def _get_rollback_operation(self, query, table, args):
        # Store the operations
        if query.__name__ == "insert":
            return lambda key: Query(table).delete(key)
        elif query.__name__ == "update":
            return lambda key: self._restore_previous_version(table, key)
        elif query.__name__ == "delete":
            return lambda key: self._restore_deleted_record(table, key)
        return lambda *args: None

    # Helper function for update rollback
    def _restore_previous_version(self, table, key):
        with self.mutex:
            columns = self._get_record_columns(table, key)
            if columns is not None:
                Query(table).update(key, *columns)


    # Helper function for delete rollback
    def _restore_deleted_record(self, table, key):
        with self.mutex:
            if key in self._deleted_records:
                Query(table).insert(*self._deleted_records[key])

    # Get the columns of a record given its primary key.
    def _get_record_columns(self, table, key):
        if self.buffer_pool is None and hasattr(table, 'database') and table.database is not None:
            self.buffer_pool = table.database.bufferpool
        rids = table.index.locate(table.key, key)
        if not rids or rids[0] not in table.page_directory:
            return None
        record = table.page_directory[rids[0]]
        return record.columns

    def run(self):
        with self.mutex:
            # Ensure transaction_id is set
            if self.transaction_id is None:
                self.transaction_id = id(self)
            
            print(f"Transaction {self.transaction_id} started with {len(self.queries)} queries")
            
            # Ensure buffer_pool and lock_manager are available
            if not self.queries:
                print("No queries to execute")
                return False
                
            if self.buffer_pool is None or self.lock_manager is None:
                _, first_table, _ = self.queries[0]
                if self.buffer_pool is None and hasattr(first_table, 'database'):
                    self.buffer_pool = first_table.database.bufferpool
                if self.lock_manager is None and hasattr(first_table, 'database'):
                    self.lock_manager = first_table.database.lock_manager
                    
            if self.lock_manager is None:
                print("Failed to get lock_manager")
                return False
                
            # Get locks, abort if fail
            for query, table, args in self.queries:
                record_id = args[0]  # Assuming first argument is record ID
                operation = query.__name__
                print(f"Acquiring {operation} lock on record {record_id}")
                
                if not self.lock_manager.acquire_lock(self.transaction_id, record_id, operation):
                    print(f"Failed to acquire lock for {operation} on record {record_id}")
                    return self.abort()  # Ensure abort returns False
                    
                self.locks_held.add(record_id)
            
            # Execute all queries
            for i, (query, table, args) in enumerate(self.queries):
                print(f"Executing query {i+1}/{len(self.queries)}: {query.__name__}")
                
                # Store the query if it is delete
                if query.__name__ == "delete":
                    columns = self._get_record_columns(table, args[0])
                    if columns is not None:
                        self._deleted_records[args[0]] = columns
                # Store the query if it is insert
                elif query.__name__ == "insert":
                    self._deleted_records[args[0]] = args
                    
                # Execute the query
                result = query(*args)
                print(f"Query result: {result}")
                
                if result is False:
                    print(f"Query {i+1} failed, aborting transaction")
                    return self.abort()  # Ensure abort returns False
                    
            print(f"All queries succeeded, committing transaction {self.transaction_id}")
            return self.commit()  # Ensure commit returns True

    def abort(self):
        # This function returns false if something is aborted
        # Roll back changes by executing rollback operations in reverse order
        with self.mutex:
            for operation, args in reversed(self.rollback_operations):
                operation(args[0])

            for record_id in self.locks_held:
                self.lock_manager.release_lock(self.transaction_id, record_id)

            self.queries.clear()
            self.rollback_operations.clear()
            self.locks_held.clear()
            self._deleted_records.clear()
            return False

    def commit(self):
        # This function returns true if commit succeeds
        with self.mutex:
            self._write_to_transaction_log()
            self._flush_dirty_pages()

            for record_id in self.locks_held:
                self.lock_manager.release_lock(self.transaction_id, record_id)
            self.queries.clear()
            self.rollback_operations.clear()
            self.locks_held.clear()
            self._deleted_records.clear()
            return True

    def _write_to_transaction_log(self):
        # Writes to the log to save all transactions
        with self.mutex:
            with open("transaction_log.txt", "a") as log_file:
                for query, table, args in self.queries:
                    log_file.write(f"Transaction {self.transaction_id}: Query {query.__name__}, Table {table.name}, Args {args}\n")
                # Flush log to disk to ensure durability
                os.fsync(log_file.fileno())

    def _flush_dirty_pages(self):
        # Flush all dirty pages if safe
        with self.mutex:
            self.buffer_pool.reset()
