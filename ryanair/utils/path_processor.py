import logging
import signal
import threading
import queue
import time
from typing import List, Dict, Any
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import execute_batch
from .path_finder import PathFinder

logger = logging.getLogger(__name__)

class PathProcessor:
    def __init__(self, db_config: Dict[str, Any], min_nights: int, max_nights: int, 
                 cutoff: int, num_threads: int = 8):
        self.db_config = db_config
        self.min_nights = min_nights
        self.max_nights = max_nights
        self.max_flights = cutoff  # Use cutoff as max_flights for PathFinder
        self.num_threads = num_threads
        self.path_queue = queue.Queue(maxsize=2000000)  # Increased from 10000 to 100000
        self.stop_event = threading.Event()
        self.db_workers = []  # List to hold multiple database worker threads
        self.paths_found = 0
        self.paths_saved = 0
        self.batch_size = 1000
        self.last_save_time = time.time()
        self.save_lock = threading.Lock()  # Lock for thread-safe path saving
        
        # Initialize database
        self.setup_database()

    def setup_database(self):
        """Drop existing tables and create new ones with minimal essential data."""
        with self.database_connection() as conn:
            with conn.cursor() as cur:
                # Drop existing tables
                cur.execute("""
                    DROP TABLE IF EXISTS path_flights CASCADE;
                    DROP TABLE IF EXISTS flights CASCADE;
                    DROP TABLE IF EXISTS paths CASCADE;
                """)
                
                # Create flights table - only essential flight information
                cur.execute("""
                    CREATE TABLE flights (
                        flight_key TEXT PRIMARY KEY,
                        origin TEXT NOT NULL,
                        destination TEXT NOT NULL,
                        departure_time BIGINT NOT NULL,
                        cost DOUBLE PRECISION NOT NULL
                    );
                    
                    CREATE INDEX idx_flights_origin_dest ON flights(origin, destination);
                """)
                
                # Create paths table - minimal path information
                cur.execute("""
                    CREATE TABLE paths (
                        path_id SERIAL PRIMARY KEY,
                        origin TEXT NOT NULL,
                        total_cost DOUBLE PRECISION NOT NULL,
                        num_flights INTEGER NOT NULL
                    );
                    
                    CREATE INDEX idx_paths_cost ON paths(total_cost);
                """)

                # Create path_flights table - minimal linking information
                cur.execute("""
                    CREATE TABLE path_flights (
                        path_id INTEGER REFERENCES paths(path_id),
                        flight_key TEXT REFERENCES flights(flight_key),
                        sequence_num INTEGER NOT NULL,
                        PRIMARY KEY (path_id, sequence_num)
                    );
                """)
                
                conn.commit()
                logger.info("Database tables recreated successfully")

    @contextmanager
    def database_connection(self):
        """Create and manage database connection with proper cleanup."""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.autocommit = False
            yield conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

    def save_paths_batch(self, conn, paths_batch: List[List[Dict]]):
        """Save a batch of paths to the database with minimal essential data."""
        try:
            with conn.cursor() as cur:
                # First insert all unique flights
                flight_data = []
                seen_flights = set()
                for path in paths_batch:
                    for flight in path:
                        flight_key = flight['key']
                        if flight_key not in seen_flights:
                            flight_data.append((
                                flight_key,
                                flight['origin'],
                                flight['destination'],
                                flight['departure'],
                                flight['cost']
                            ))
                            seen_flights.add(flight_key)

                if flight_data:
                    execute_batch(cur, """
                        INSERT INTO flights (
                            flight_key, origin, destination, departure_time, cost
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (flight_key) DO NOTHING
                    """, flight_data)

                # Prepare all path data for batch insertion
                path_data = []
                path_flights_data = []
                
                for path in paths_batch:
                    total_cost = sum(f['cost'] for f in path)
                    
                    # Insert path
                    cur.execute("""
                        INSERT INTO paths (origin, total_cost, num_flights)
                        VALUES (%s, %s, %s)
                        RETURNING path_id
                    """, (
                        path[0]['origin'],
                        total_cost,
                        len(path)
                    ))
                    
                    path_id = cur.fetchone()[0]
                    
                    # Collect path_flights data for batch insertion
                    for i, flight in enumerate(path):
                        path_flights_data.append((
                            path_id,
                            flight['key'],
                            i
                        ))
                
                # Batch insert path_flights
                if path_flights_data:
                    execute_batch(cur, """
                        INSERT INTO path_flights (path_id, flight_key, sequence_num)
                        VALUES (%s, %s, %s)
                    """, path_flights_data)

                conn.commit()
                self.paths_saved += len(paths_batch)
                
                current_time = time.time()
                elapsed = current_time - self.last_save_time
                logger.info(f"Saved {len(paths_batch)} paths. Total saved: {self.paths_saved}. "
                          f"Rate: {len(paths_batch)/elapsed:.2f} paths/sec")
                self.last_save_time = current_time

        except Exception as e:
            logger.error(f"Error saving paths batch: {e}")
            conn.rollback()
            raise

    def database_worker(self, worker_id: int):
        """Worker thread that saves paths to the database in batches."""
        logger.info(f"Database worker {worker_id} started")
        current_batch = []
        
        with self.database_connection() as conn:
            while not self.stop_event.is_set() or not self.path_queue.empty():
                try:
                    # Get paths with timeout to check stop event periodically
                    try:
                        path = self.path_queue.get(timeout=1.0)
                        current_batch.append(path)
                    except queue.Empty:
                        if current_batch:  # Save partial batch before checking stop
                            with self.save_lock:
                                self.save_paths_batch(conn, current_batch)
                            current_batch = []
                        continue

                    # Save batch when it reaches the target size
                    if len(current_batch) >= self.batch_size:
                        with self.save_lock:
                            self.save_paths_batch(conn, current_batch)
                        current_batch = []

                except Exception as e:
                    logger.error(f"Error in database worker {worker_id}: {e}")
                    if current_batch:
                        logger.error(f"Lost batch of {len(current_batch)} paths")
                    current_batch = []

            # Save any remaining paths
            if current_batch:
                try:
                    with self.save_lock:
                        self.save_paths_batch(conn, current_batch)
                except Exception as e:
                    logger.error(f"Error saving final batch in worker {worker_id}: {e}")

        logger.info(f"Database worker {worker_id} finished")

    def path_callback(self, path):
        """Callback function that receives paths from the C++ code."""
        try:
            # Convert path to list of dicts for easier handling
            path_data = []
            for flight in path:
                path_data.append({
                    'key': flight['key'],
                    'origin': flight['origin'],
                    'destination': flight['destination'],
                    'departure': flight['departure'],
                    'arrival': flight['arrival'],
                    'cost': flight['cost'],
                    'currency': flight['currency']
                })
            
            # Try to add to queue with timeout to prevent blocking
            try:
                self.path_queue.put(path_data)
                self.paths_found += 1
                if self.paths_found % 1000 == 0:
                    queue_size = self.path_queue.qsize()
                    logger.info(f"Found {self.paths_found} paths. Queue size: {queue_size}")
            except queue.Full:
                logger.warning("Path queue is full, skipping path")

        except Exception as e:
            logger.error(f"Error in path callback: {e}")

    def process_paths(self, graph_path: str, origin: str) -> int:
        """Main method to process paths for a given origin."""
        logger.info(f"Starting path processing for origin: {origin}")
        
        # Start multiple database worker threads
        self.stop_event.clear()
        num_db_workers = min(4, self.num_threads)  # Use up to 4 database workers
        self.db_workers = []
        for i in range(num_db_workers):
            worker = threading.Thread(target=self.database_worker, args=(i,))
            worker.start()
            self.db_workers.append(worker)
        
        logger.info(f"Started {num_db_workers} database worker threads")

        try:
            # Initialize path finder
            finder = PathFinder(origin, self.min_nights, self.max_nights, self.max_flights)
            finder.load_graph(graph_path)
            
            # Set up signal handling
            def signal_handler(signum, frame):
                logger.info("Received shutdown signal")
                finder.stop_processing()
                self.stop_event.set()
            
            original_handlers = {}
            for sig in (signal.SIGINT, signal.SIGTERM):
                original_handlers[sig] = signal.signal(sig, signal_handler)

            try:
                # Process paths
                total_paths = finder.process(self.path_callback)
                logger.info(f"Path finding completed. Found {total_paths} paths.")
            finally:
                # Restore original signal handlers
                for sig, handler in original_handlers.items():
                    signal.signal(sig, handler)

        except Exception as e:
            logger.error(f"Error processing paths: {e}")
            raise
        finally:
            # Signal database workers to stop and wait for them
            self.stop_event.set()
            for worker in self.db_workers:
                worker.join()
            logger.info(f"Processing completed. Saved {self.paths_saved} paths to database.")

        return self.paths_saved 