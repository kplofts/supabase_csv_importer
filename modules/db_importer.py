"""
Database Importer Module
Handles all database operations with optimization for large imports
"""
import os
import io
import csv
import time
import psycopg2
import psycopg2.pool
import psycopg2.extras
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from .progress_tracker import ProgressTracker
from .utils import log_message, setup_logging, format_bytes, detect_encoding


class DatabaseImporter:
    """Optimized database importer for Supabase/PostgreSQL"""
    
    def __init__(self, config: Dict[str, Any], progress_tracker: ProgressTracker):
        self.config = config
        self.progress = progress_tracker
        self.log_file = None
        self.connection_pool = None
        self._setup_logging()
        self._initialize_connection_pool()
        
    def _setup_logging(self):
        """Setup logging for the importer"""
        log_dir = self.config['logging']['directory']
        self.log_file = setup_logging(log_dir, 'db_import')
        
    def _initialize_connection_pool(self):
        """Initialize database connection pool"""
        try:
            pool_config = self.config['database']['pool']
            
            # Check if using connection string or individual parameters
            if 'connection_string' in self.config['database']:
                # Use connection string
                connection_string = self.config['database']['connection_string']
                
                # Add schema to connection string if not present
                if 'options=' not in connection_string:
                    schema = self.config['database'].get('schema', 'public')
                    connection_string += f"?options=-csearch_path%3D{schema}"
                
                self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                    pool_config['min_connections'],
                    pool_config['max_connections'],
                    connection_string,
                    keepalives=1,
                    keepalives_idle=pool_config['keepalive'],
                    keepalives_interval=10,
                    keepalives_count=5
                )
            else:
                # Use individual parameters (backward compatibility)
                self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                    pool_config['min_connections'],
                    pool_config['max_connections'],
                    host=self.config['database']['host'],
                    port=self.config['database']['port'],
                    database=self.config['database']['database'],
                    user=self.config['database']['user'],
                    password=self.config['database']['password'],
                    options=f"-c search_path={self.config['database']['schema']}",
                    keepalives=1,
                    keepalives_idle=pool_config['keepalive'],
                    keepalives_interval=10,
                    keepalives_count=5
                )
            
            log_message(self.log_file, f"Connection pool initialized with {pool_config['max_connections']} connections")
        except Exception as e:
            log_message(self.log_file, f"Failed to initialize connection pool: {e}")
            raise
    
    def optimize_for_import(self) -> None:
        """Optimize database settings for bulk import"""
        conn = self.connection_pool.getconn()
        try:
            with conn.cursor() as cursor:
                optimization_config = self.config['optimization']
                table_name = self.config['database']['table_name']
                
                # Store current settings
                cursor.execute("""
                    SELECT current_setting('work_mem') as work_mem,
                           current_setting('maintenance_work_mem') as maintenance_work_mem,
                           current_setting('checkpoint_completion_target') as checkpoint_completion_target,
                           current_setting('statement_timeout') as statement_timeout
                """)
                self.original_settings = cursor.fetchone()
                
                # Apply optimizations
                timeout = optimization_config.get('statement_timeout', '30min')
                optimizations = [
                    "SET work_mem = '256MB'",
                    "SET maintenance_work_mem = '1GB'",
                    "SET checkpoint_completion_target = 0.9",
                    "SET synchronous_commit = OFF",
                    "SET wal_writer_delay = '200ms'",
                    f"SET statement_timeout = '{timeout}'"
                ]
                
                for optimization in optimizations:
                    cursor.execute(optimization)
                    log_message(self.log_file, f"Applied: {optimization}")
                
                # Disable triggers if configured
                if optimization_config.get('disable_triggers', False):
                    cursor.execute(f"ALTER TABLE {table_name} DISABLE TRIGGER ALL")
                    log_message(self.log_file, f"Disabled triggers on {table_name}")
                
                conn.commit()
                log_message(self.log_file, "Database optimized for import")
                
        except Exception as e:
            log_message(self.log_file, f"Error optimizing database: {e}")
            conn.rollback()
        finally:
            self.connection_pool.putconn(conn)
    
    def restore_after_import(self) -> None:
        """Restore database settings after import"""
        conn = self.connection_pool.getconn()
        try:
            with conn.cursor() as cursor:
                optimization_config = self.config['optimization']
                table_name = self.config['database']['table_name']
                
                # Re-enable triggers
                if optimization_config.get('disable_triggers', False):
                    cursor.execute(f"ALTER TABLE {table_name} ENABLE TRIGGER ALL")
                    log_message(self.log_file, f"Re-enabled triggers on {table_name}")
                
                # Run maintenance commands
                if optimization_config.get('run_vacuum', True):
                    cursor.execute(f"VACUUM ANALYZE {table_name}")
                    log_message(self.log_file, f"Ran VACUUM ANALYZE on {table_name}")
                elif optimization_config.get('run_analyze', True):
                    cursor.execute(f"ANALYZE {table_name}")
                    log_message(self.log_file, f"Ran ANALYZE on {table_name}")
                
                conn.commit()
                log_message(self.log_file, "Database settings restored")
                
        except Exception as e:
            log_message(self.log_file, f"Error restoring database settings: {e}")
            conn.rollback()
        finally:
            self.connection_pool.putconn(conn)
    
    def import_files(self, file_paths: List[str], use_parallel: bool = True, 
                    batch_size: int = 10000) -> bool:
        """Import multiple CSV files"""
        if not file_paths:
            return True
        
        total_files = len(file_paths)
        log_message(self.log_file, f"Starting import of {total_files} files")
        
        if use_parallel and total_files > 1:
            return self._import_files_parallel(file_paths, batch_size)
        else:
            return self._import_files_sequential(file_paths, batch_size)
    
    def _import_files_sequential(self, file_paths: List[str], batch_size: int) -> bool:
        """Import files sequentially"""
        success_count = 0
        
        for i, file_path in enumerate(file_paths, 1):
            self.progress.update_status(f"Importing file {i}/{len(file_paths)}")
            
            if self._import_single_file(file_path, batch_size):
                success_count += 1
            else:
                log_message(self.log_file, f"Failed to import {file_path}")
        
        return success_count == len(file_paths)
    
    def _import_files_parallel(self, file_paths: List[str], batch_size: int) -> bool:
        """Import files in parallel"""
        max_workers = min(
            self.config['import']['parallel_workers'],
            self.config['database']['pool']['max_connections'] - 1
        )
        
        log_message(self.log_file, f"Using {max_workers} parallel workers")
        
        success_count = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._import_single_file, file_path, batch_size): file_path
                for file_path in file_paths
            }
            
            for future in as_completed(futures):
                file_path = futures[future]
                try:
                    if future.result():
                        success_count += 1
                except Exception as e:
                    log_message(self.log_file, f"Error importing {file_path}: {e}")
        
        return success_count == len(file_paths)
    
    def _import_single_file(self, file_path: str, batch_size: int) -> bool:
        """Import a single CSV file using COPY command"""
        conn = self.connection_pool.getconn()
        start_time = time.time()
        rows_imported = 0
        
        try:
            # Set a longer timeout for this specific connection
            timeout = self.config.get('optimization', {}).get('statement_timeout', '30min')
            with conn.cursor() as cursor:
                cursor.execute(f"SET statement_timeout = '{timeout}'")
            
            file_size = os.path.getsize(file_path)
            encoding = detect_encoding(file_path)
            
            log_message(self.log_file, f"Importing {file_path} ({format_bytes(file_size)})")
            
            with open(file_path, 'r', encoding=encoding) as f:
                # Read header
                header = f.readline().strip()
                columns = [col.strip().strip('"') for col in header.split(',')]
                
                # Create COPY command
                table_name = self.config['database']['table_name']
                copy_sql = f"""
                    COPY {table_name} ({','.join(columns)})
                    FROM STDIN WITH (FORMAT CSV, HEADER FALSE, DELIMITER ',', QUOTE '"')
                """
                
                with conn.cursor() as cursor:
                    # Use COPY for maximum performance
                    cursor.copy_expert(sql=copy_sql, file=f)
                    rows_imported = cursor.rowcount
                    
                    conn.commit()
                    
                    # Update progress
                    self.progress.add_rows(rows_imported)
                    self.progress.add_bytes(file_size)
                    
                    elapsed = time.time() - start_time
                    rows_per_sec = rows_imported / elapsed if elapsed > 0 else 0
                    
                    log_message(
                        self.log_file,
                        f"Imported {rows_imported:,} rows from {os.path.basename(file_path)} "
                        f"in {elapsed:.1f}s ({rows_per_sec:,.0f} rows/sec)"
                    )
                    
                    return True
                    
        except Exception as e:
            log_message(self.log_file, f"Error importing {file_path}: {e}")
            conn.rollback()
            
            # Try fallback method with batch inserts
            if "COPY" in str(e):
                log_message(self.log_file, "Falling back to batch INSERT method")
                return self._import_with_batch_insert(file_path, batch_size, conn)
            
            return False
            
        finally:
            self.connection_pool.putconn(conn)
    
    def _import_with_batch_insert(self, file_path: str, batch_size: int, conn) -> bool:
        """Fallback import method using batch INSERT"""
        try:
            encoding = detect_encoding(file_path)
            rows_imported = 0
            
            with open(file_path, 'r', encoding=encoding) as f:
                reader = csv.DictReader(f)
                
                with conn.cursor() as cursor:
                    batch = []
                    
                    for row in reader:
                        batch.append(row)
                        
                        if len(batch) >= batch_size:
                            self._insert_batch(cursor, batch)
                            rows_imported += len(batch)
                            self.progress.add_rows(len(batch))
                            batch = []
                    
                    # Insert remaining rows
                    if batch:
                        self._insert_batch(cursor, batch)
                        rows_imported += len(batch)
                        self.progress.add_rows(len(batch))
                    
                    conn.commit()
                    
            log_message(self.log_file, f"Imported {rows_imported:,} rows using batch INSERT")
            return True
            
        except Exception as e:
            log_message(self.log_file, f"Batch INSERT failed: {e}")
            conn.rollback()
            return False
    
    def _insert_batch(self, cursor, batch: List[Dict[str, Any]]) -> None:
        """Insert a batch of rows"""
        if not batch:
            return
        
        table_name = self.config['database']['table_name']
        columns = list(batch[0].keys())
        
        # Build INSERT statement
        placeholders = ','.join(['%s'] * len(columns))
        insert_sql = f"""
            INSERT INTO {table_name} ({','.join(columns)})
            VALUES ({placeholders})
        """
        
        # Execute batch insert
        values = [[row.get(col) for col in columns] for row in batch]
        psycopg2.extras.execute_batch(cursor, insert_sql, values, page_size=len(batch))
    
    def get_row_count(self) -> int:
        """Get current row count in target table"""
        conn = self.connection_pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {self.config['database']['table_name']}")
                return cursor.fetchone()[0]
        finally:
            self.connection_pool.putconn(conn)
    
    def cleanup(self):
        """Cleanup resources"""
        if self.connection_pool:
            self.connection_pool.closeall()
            log_message(self.log_file, "Connection pool closed")