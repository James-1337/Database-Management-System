from threading import Thread
from lstore.table import Table, Record
from lstore.index import Index

class TransactionWorker:
    """
    Manage and execute multiple transactions concurrently using threads
    """

    def __init__(self, transactions=None):
        """
        Creates a TransactionWorker object

        transactions (list): A list of transactions to be executed by this worker
        """
        self.stats = []  # List to store  result of each transaction (True if committed, False if aborted)
        self.transactions = transactions if transactions is not None else []  # List of transactions to be executed
        self.result = 0  # Number of transactions that committed successfully
        self.thread = None  # Thread object for running transactions concurrently

    def add_transaction(self, t):
        """
        Adds a transaction to list of transactions to be executed

        t is the transaction to be added
        """
        self.transactions.append(t)

    def run(self):
        """
        Starts execution of all transactions in a separate thread
        """
        # print(f"Creating worker thread with {len(self.transactions)} transactions")
        self.thread = Thread(target=self.__run)  # Create a thread to run transactions
        self.thread.start()
        # print("Worker thread started")

    def join(self):
        """
        Waits for worker thread to finish execution
        """
        if self.thread:
            self.thread.join()  # Wait for thread to complete

    def __run(self):
        """
        Private method to execute all transactions and record their results
        """
        # print(f"Worker started, processing {len(self.transactions)} transactions")
        for i, transaction in enumerate(self.transactions):
            # print(f"Starting transaction {i+1}/{len(self.transactions)}")
            try:
                # Execute each transaction and record whether it committed or aborted
                result = transaction.run()
                # print(f"Transaction {i+1} completed with result: {result}")
                self.stats.append(True if result is True else False)
            except Exception as e:
                print(f"Transaction {i+1} failed with error: {e}")
                self.stats.append(False)
        
        # Calculate number of transactions that committed successfully
        self.result = sum(1 for x in self.stats if x is True)
        # print(f"Worker completed, {self.result} transactions committed successfully")
