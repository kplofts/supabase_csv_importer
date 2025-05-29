#!/usr/bin/env python3
"""
Generate test CSV files for the Supabase importer
Creates realistic data with configurable size
"""
import csv
import random
import string
import time
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any


class TestCSVGenerator:
    """Generate test CSV files with realistic data"""
    
    def __init__(self):
        self.start_date = datetime(2020, 1, 1)
        self.end_date = datetime(2024, 12, 31)
        
        # Sample data for realistic content
        self.first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", 
                           "Michael", "Linda", "William", "Elizabeth", "David", "Barbara",
                           "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah"]
        
        self.last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
                          "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
                          "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore"]
        
        self.cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
                      "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
                      "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte"]
        
        self.states = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA",
                      "TX", "FL", "TX", "OH", "NC"]
        
        self.categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books",
                          "Toys", "Health", "Beauty", "Automotive", "Food"]
        
        self.statuses = ["active", "pending", "completed", "cancelled", "processing"]
    
    def generate_row(self, row_id: int) -> Dict[str, Any]:
        """Generate a single row of test data"""
        # Random date
        days_between = (self.end_date - self.start_date).days
        random_days = random.randint(0, days_between)
        created_date = self.start_date + timedelta(days=random_days)
        modified_date = created_date + timedelta(days=random.randint(0, 30))
        
        # Generate data
        return {
            'id': row_id,
            'created_at': created_date.isoformat(),
            'modified_at': modified_date.isoformat(),
            'first_name': random.choice(self.first_names),
            'last_name': random.choice(self.last_names),
            'email': f"user{row_id}@example.com",
            'phone': f"+1-{random.randint(200,999)}-{random.randint(200,999)}-{random.randint(1000,9999)}",
            'address': f"{random.randint(1, 9999)} {random.choice(['Main', 'Oak', 'Elm', 'Park', 'First'])} Street",
            'city': random.choice(self.cities),
            'state': self.states[self.cities.index(self.cities[random.randint(0, len(self.cities)-1)]) % len(self.states)],
            'zip_code': f"{random.randint(10000, 99999)}",
            'category': random.choice(self.categories),
            'amount': round(random.uniform(10.0, 1000.0), 2),
            'quantity': random.randint(1, 100),
            'status': random.choice(self.statuses),
            'is_active': random.choice(['true', 'false']),
            'notes': self._generate_text(random.randint(10, 50)),
            'tags': ','.join(random.sample(self.categories, random.randint(1, 3))),
            'score': round(random.uniform(0, 100), 2),
            'reference_id': f"REF-{row_id:08d}",
            'external_id': ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))
        }
    
    def _generate_text(self, word_count: int) -> str:
        """Generate random text with specified word count"""
        words = []
        lorem_words = ["lorem", "ipsum", "dolor", "sit", "amet", "consectetur", 
                       "adipiscing", "elit", "sed", "do", "eiusmod", "tempor",
                       "incididunt", "ut", "labore", "et", "dolore", "magna", "aliqua"]
        
        for _ in range(word_count):
            words.append(random.choice(lorem_words))
        
        return ' '.join(words)
    
    def generate_csv(self, filename: str, target_size_mb: int, show_progress: bool = True):
        """Generate a CSV file of approximately the target size"""
        print(f"🔧 Generating {target_size_mb}MB test CSV file...")
        print(f"📄 Output file: {filename}")
        
        target_size_bytes = target_size_mb * 1024 * 1024
        current_size = 0
        row_count = 0
        start_time = time.time()
        
        # Create CSV file
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            # First, generate a sample row to get column names
            sample_row = self.generate_row(1)
            fieldnames = list(sample_row.keys())
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            # Track file position for size calculation
            header_size = f.tell()
            current_size = header_size
            
            # Generate rows until we reach target size
            while current_size < target_size_bytes:
                row_count += 1
                row = self.generate_row(row_count)
                
                # Write row
                writer.writerow(row)
                
                # Update size (approximate)
                current_size = f.tell()
                
                # Progress update
                if show_progress and row_count % 10000 == 0:
                    progress_pct = (current_size / target_size_bytes) * 100
                    elapsed = time.time() - start_time
                    rows_per_sec = row_count / elapsed if elapsed > 0 else 0
                    
                    print(f"  Progress: {progress_pct:.1f}% | "
                          f"Rows: {row_count:,} | "
                          f"Size: {current_size / (1024*1024):.1f}MB | "
                          f"Speed: {rows_per_sec:.0f} rows/sec")
        
        # Final stats
        elapsed = time.time() - start_time
        final_size = os.path.getsize(filename)
        final_size_mb = final_size / (1024 * 1024)
        
        print(f"\n✅ CSV generation complete!")
        print(f"📊 Final Statistics:")
        print(f"  • File size: {final_size_mb:.2f} MB")
        print(f"  • Total rows: {row_count:,}")
        print(f"  • Time elapsed: {elapsed:.1f} seconds")
        print(f"  • Average row size: {final_size / row_count:.0f} bytes")
        print(f"  • Generation speed: {row_count / elapsed:.0f} rows/sec")
        
        return filename


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Generate test CSV files for Supabase importer testing"
    )
    
    parser.add_argument(
        "size",
        type=int,
        help="Target file size in MB"
    )
    
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Output filename (default: test_data_[SIZE]mb.csv)"
    )
    
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Suppress progress output"
    )
    
    args = parser.parse_args()
    
    # Generate filename if not provided
    if args.output is None:
        args.output = f"test_data_{args.size}mb.csv"
    
    # Create generator and generate file
    generator = TestCSVGenerator()
    generator.generate_csv(
        filename=args.output,
        target_size_mb=args.size,
        show_progress=not args.quiet
    )


if __name__ == "__main__":
    main()