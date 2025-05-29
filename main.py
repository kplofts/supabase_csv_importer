#!/usr/bin/env python3
"""
Supabase CSV Importer - Optimized for Large Files
Main entry point with CLI interface
"""
import os
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any

from modules.config_manager import ConfigManager
from modules.file_analyzer import FileAnalyzer
from modules.file_splitter import FileSplitter
from modules.db_importer import DatabaseImporter
from modules.progress_tracker import ProgressTracker
from modules.performance_optimizer import PerformanceOptimizer
from modules.utils import (
    setup_logging, log_message, format_bytes, format_duration,
    print_banner, print_summary, validate_environment
)


class SupabaseImporter:
    """Main orchestrator for the import process"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_manager = ConfigManager(config_path)
        self.config = self.config_manager.load_config()
        self.progress = ProgressTracker()
        self.start_time = None
        self.log_file = None
        
    def import_file(self, file_path: str, options: Dict[str, Any]) -> bool:
        """Import a single CSV file to Supabase"""
        try:
            self.start_time = time.time()
            
            # Setup logging
            log_dir = self.config['logging']['directory']
            self.log_file = setup_logging(log_dir, 'import')
            
            log_message(self.log_file, "="*80)
            log_message(self.log_file, f"IMPORT STARTED: {file_path}")
            log_message(self.log_file, "="*80)
            
            # Step 1: Analyze file
            print("\n📊 Analyzing CSV file...")
            analyzer = FileAnalyzer(self.config)
            analysis = analyzer.analyze_file(file_path)
            
            if not analysis['valid']:
                print(f"❌ Invalid CSV file: {analysis.get('error', 'Unknown error')}")
                return False
            
            self._print_analysis(analysis)
            
            # Step 2: Determine if splitting is needed
            if analysis['size_mb'] > self.config['import']['chunk_size_mb']:
                print(f"\n✂️  File is {analysis['size_mb']:.1f}MB, splitting into chunks...")
                
                splitter = FileSplitter(self.config)
                chunk_files = splitter.split_file(
                    file_path,
                    chunk_size_mb=options.get('chunk_size', self.config['import']['chunk_size_mb']),
                    output_dir=options.get('temp_dir', self.config['directories']['temp_directory'])
                )
                
                print(f"✅ Split into {len(chunk_files)} chunks")
                files_to_import = chunk_files
            else:
                print(f"\n📄 File is small enough ({analysis['size_mb']:.1f}MB), importing directly...")
                files_to_import = [file_path]
            
            # Step 3: Import files
            print(f"\n🚀 Starting import to {self.config['database']['table_name']}...")
            
            importer = DatabaseImporter(self.config, self.progress)
            
            # Optimize database for import
            if options.get('optimize', True):
                print("⚡ Optimizing database for import...")
                importer.optimize_for_import()
            
            # Import all files
            success = importer.import_files(
                files_to_import,
                use_parallel=options.get('parallel', True),
                batch_size=options.get('batch_size', self.config['import']['batch_size'])
            )
            
            # Restore database settings
            if options.get('optimize', True):
                print("🔧 Restoring database settings...")
                importer.restore_after_import()
            
            # Step 4: Cleanup temporary files
            if len(files_to_import) > 1:  # We created chunks
                print("\n🧹 Cleaning up temporary files...")
                for chunk_file in files_to_import:
                    try:
                        os.remove(chunk_file)
                    except:
                        pass
            
            # Step 5: Print summary
            elapsed_time = time.time() - self.start_time
            self._print_summary(analysis, success, elapsed_time)
            
            return success
            
        except Exception as e:
            log_message(self.log_file, f"FATAL ERROR: {e}")
            print(f"\n❌ Fatal error: {e}")
            return False
    
    def import_directory(self, directory_path: str, options: Dict[str, Any]) -> bool:
        """Import all CSV files in a directory"""
        try:
            csv_files = list(Path(directory_path).glob("*.csv"))
            if not csv_files:
                print(f"❌ No CSV files found in {directory_path}")
                return False
            
            print(f"\n📁 Found {len(csv_files)} CSV files to import")
            
            total_size = sum(f.stat().st_size for f in csv_files)
            print(f"📊 Total size: {format_bytes(total_size)}")
            
            # Sort by size (largest first for better parallelization)
            csv_files.sort(key=lambda f: f.stat().st_size, reverse=True)
            
            success_count = 0
            for i, file_path in enumerate(csv_files, 1):
                print(f"\n{'='*80}")
                print(f"Processing file {i}/{len(csv_files)}: {file_path.name}")
                print(f"{'='*80}")
                
                if self.import_file(str(file_path), options):
                    success_count += 1
                else:
                    print(f"⚠️  Failed to import {file_path.name}")
            
            print(f"\n{'='*80}")
            print(f"✅ Successfully imported {success_count}/{len(csv_files)} files")
            print(f"{'='*80}")
            
            return success_count == len(csv_files)
            
        except Exception as e:
            print(f"\n❌ Error processing directory: {e}")
            return False
    
    def _print_analysis(self, analysis: Dict[str, Any]) -> None:
        """Print file analysis results"""
        print(f"\n📋 File Analysis:")
        print(f"  • Size: {format_bytes(analysis['size_bytes'])} ({analysis['size_mb']:.1f} MB)")
        print(f"  • Rows: {analysis['row_count']:,}")
        print(f"  • Columns: {analysis['column_count']}")
        print(f"  • Estimated chunks: {analysis['estimated_chunks']}")
        print(f"  • Encoding: {analysis['encoding']}")
        
        if analysis['sample_rows']:
            print(f"  • Sample data: ✓ ({len(analysis['sample_rows'])} rows analyzed)")
    
    def _print_summary(self, analysis: Dict[str, Any], success: bool, elapsed_time: float) -> None:
        """Print import summary"""
        print(f"\n{'='*80}")
        print("📊 IMPORT SUMMARY")
        print(f"{'='*80}")
        
        if success:
            print(f"✅ Status: SUCCESS")
        else:
            print(f"❌ Status: FAILED")
        
        print(f"⏱️  Duration: {format_duration(elapsed_time)}")
        print(f"📈 Rows imported: {self.progress.total_rows:,}")
        print(f"💾 Data processed: {format_bytes(self.progress.bytes_processed)}")
        
        if elapsed_time > 0:
            rows_per_sec = self.progress.total_rows / elapsed_time
            mb_per_sec = (self.progress.bytes_processed / (1024*1024)) / elapsed_time
            print(f"⚡ Performance: {rows_per_sec:,.0f} rows/sec, {mb_per_sec:.1f} MB/sec")
        
        # Cost estimation (rough estimate based on Supabase pricing)
        storage_gb = self.progress.bytes_processed / (1024**3)
        estimated_cost = storage_gb * 0.125  # $0.125 per GB
        print(f"💰 Estimated cost: ${estimated_cost:.4f}")
        
        print(f"{'='*80}")


def main():
    """Main entry point"""
    print_banner()
    
    parser = argparse.ArgumentParser(
        description="Supabase CSV Importer - Optimized for large files",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "path",
        help="Path to CSV file or directory"
    )
    
    parser.add_argument(
        "-c", "--config",
        default="config.yaml",
        help="Path to config file (default: config.yaml)"
    )
    
    parser.add_argument(
        "--optimize",
        action="store_true",
        help="Run performance optimization wizard"
    )
    
    parser.add_argument(
        "--instance-size",
        type=int,
        choices=range(1, 12),
        help="Supabase instance size (1=Nano to 11=16XL)"
    )
    
    parser.add_argument(
        "--performance-level",
        type=int,
        choices=[1, 2, 3],
        help="Performance level: 1=Conservative, 2=Balanced, 3=Aggressive"
    )
    
    parser.add_argument(
        "--chunk-size",
        type=int,
        help="Chunk size in MB (overrides optimization)"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Batch size for imports (overrides optimization)"
    )
    
    parser.add_argument(
        "--no-parallel",
        action="store_true",
        help="Disable parallel processing"
    )
    
    parser.add_argument(
        "--no-optimize-db",
        action="store_true",
        help="Skip database optimization"
    )
    
    parser.add_argument(
        "--temp-dir",
        help="Temporary directory for chunks"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Analyze files without importing"
    )
    
    args = parser.parse_args()
    
    # Validate environment
    if not validate_environment():
        return 1
    
    # Check if path exists
    path = Path(args.path)
    if not path.exists():
        print(f"❌ Error: Path '{args.path}' does not exist")
        return 1
    
    # Create importer
    importer = SupabaseImporter(args.config)
    
    # Run optimization wizard if requested
    if args.optimize or (args.instance_size and args.performance_level):
        optimizer = PerformanceOptimizer()
        
        if args.optimize:
            # Interactive mode
            print("\n🔧 PERFORMANCE OPTIMIZATION WIZARD")
            print("="*50)
            print("\nSelect your Supabase instance size:")
            for i, spec in optimizer.INSTANCE_SPECS.items():
                print(f"  {i:2d}. {spec['name']:6s} ({spec['memory_gb']:3g}GB RAM, {spec['cpu_cores']:2d} cores)")
            
            instance_size = int(input("\nEnter instance size (1-11): "))
            
            print("\nSelect performance level:")
            for i, level in optimizer.PERFORMANCE_LEVELS.items():
                print(f"  {i}. {level['name']}")
            
            performance_level = int(input("\nEnter performance level (1-3): "))
        else:
            instance_size = args.instance_size
            performance_level = args.performance_level
        
        # Get optimized settings
        optimized = optimizer.get_optimized_settings(instance_size, performance_level)
        optimizer.print_optimization_summary(optimized, instance_size, performance_level)
        
        # Apply optimized settings to config
        importer.config['database']['pool'].update(optimized['database']['pool'])
        importer.config['import'].update(optimized['import'])
        importer.config['optimization'].update(optimized['optimization'])
        
        # Ask for confirmation
        if input("\nApply these settings? (y/n): ").lower() != 'y':
            print("Settings not applied. Using config file defaults.")
        else:
            print("✅ Optimized settings applied!")
    
    # Prepare options (command line args override optimized settings)
    options = {
        'chunk_size': args.chunk_size or importer.config['import']['chunk_size_mb'],
        'batch_size': args.batch_size or importer.config['import']['batch_size'],
        'parallel': not args.no_parallel,
        'optimize': not args.no_optimize_db,
        'temp_dir': args.temp_dir,
        'dry_run': args.dry_run
    }
    
    # Import file or directory
    try:
        if path.is_file():
            success = importer.import_file(str(path), options)
        elif path.is_dir():
            success = importer.import_directory(str(path), options)
        else:
            print(f"❌ Error: '{args.path}' is neither a file nor a directory")
            return 1
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Import cancelled by user")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())