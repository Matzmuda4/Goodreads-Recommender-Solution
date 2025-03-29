import os
import gzip
import json
import pandas as pd
from collections import defaultdict

# Set directory paths
DATASET_DIR = './Goodreads Dataset'

def get_dataset_files():
    """Return all .json.gz and .csv files in the dataset directory."""
    files = []
    for file in os.listdir(DATASET_DIR):
        if file.endswith('.json.gz') or file.endswith('.csv'):
            files.append(os.path.join(DATASET_DIR, file))
    return files

def explore_file(file_path, num_samples=5):
    """Explore a JSON file and return sample records."""
    filename = os.path.basename(file_path)
    print(f"\n{'='*80}\nExploring: {filename}\n{'='*80}")
    
    samples = []
    field_types = defaultdict(set)
    field_counts = defaultdict(int)
    
    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= num_samples:
                    break
                try:
                    data = json.loads(line)
                    samples.append(data)
                    # Collect field information
                    for field, value in data.items():
                        field_counts[field] += 1
                        field_types[field].add(type(value).__name__)
                except json.JSONDecodeError:
                    print(f"Error parsing JSON at line {i+1}")
    except Exception as e:
        print(f"Error opening file: {str(e)}")
        return None, None
    
    # Print field information
    print(f"Fields found in {filename}:")
    for field, count in sorted(field_counts.items(), key=lambda x: x[1], reverse=True):
        types = ", ".join(field_types[field])
        print(f"  {field}: {count}/{num_samples} records ({types})")
    
    # Print samples in a readable format
    for i, sample in enumerate(samples):
        print(f"\nSample {i+1}:")
        for k, v in sample.items():
            # Format complex values for readability
            if isinstance(v, dict) and len(v) > 3:
                v = f"{list(v.keys())[:3]}... ({len(v)} items)"
            elif isinstance(v, list) and len(v) > 3:
                if v and isinstance(v[0], dict):
                    v = f"[{v[0]}... + {len(v)-1} more]"
                else:
                    v = f"{v[:3]}... ({len(v)} items)"
            elif isinstance(v, str) and len(v) > 100:
                v = v[:100] + "..."
            print(f"  {k}: {v}")
    
    return samples, field_counts

def explore_csv_file(file_path, num_samples=5):
    """Explore a CSV file and return sample records."""
    filename = os.path.basename(file_path)
    print(f"\n{'='*80}\nExploring: {filename}\n{'='*80}")
    
    try:
        df = pd.read_csv(file_path, nrows=num_samples)
    except Exception as e:
        print(f"Error reading CSV file: {str(e)}")
        return None, None
    
    # Print column information and data types
    print(f"Columns in {filename}:")
    for col in df.columns:
        dtype = df[col].dtype
        print(f"  {col}: {dtype}")
    
    samples = df.to_dict(orient='records')
    
    # Print sample rows
    for i, sample in enumerate(samples):
        print(f"\nSample {i+1}:")
        for k, v in sample.items():
            print(f"  {k}: {v}")
    
    return samples, df.dtypes.to_dict()

def find_id_connections():
    """Analyze how different files connect through IDs."""
    files = get_dataset_files()
    
    # Identify relevant files based on filename keywords
    book_files = [f for f in files if 'book' in os.path.basename(f).lower()]
    author_files = [f for f in files if 'author' in os.path.basename(f).lower()]
    book_author_files = [f for f in files if 'book' in os.path.basename(f).lower() and 'author' in os.path.basename(f).lower()]
    
    print("\n\n=== ID CONNECTION ANALYSIS ===")
    
    # Check book IDs
    if book_files:
        print("\nSampling book IDs:")
        file = book_files[0]
        if file.endswith('.json.gz'):
            with gzip.open(file, 'rt', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i >= 10:
                        break
                    try:
                        data = json.loads(line)
                        book_id = data.get('book_id', data.get('asin', ''))
                        if book_id:
                            print(f"  Book ID: {book_id}")
                    except:
                        continue
        elif file.endswith('.csv'):
            try:
                df = pd.read_csv(file, nrows=10)
                for _, row in df.iterrows():
                    data = row.to_dict()
                    book_id = data.get('book_id', data.get('asin', ''))
                    if book_id:
                        print(f"  Book ID: {book_id}")
            except Exception as e:
                print(f"Error reading CSV file {file}: {str(e)}")
    
    # Check author IDs
    if author_files:
        print("\nSampling author IDs:")
        file = author_files[0]
        if file.endswith('.json.gz'):
            with gzip.open(file, 'rt', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i >= 10:
                        break
                    try:
                        data = json.loads(line)
                        author_id = data.get('author_id', '')
                        if author_id:
                            print(f"  Author ID: {author_id}")
                    except:
                        continue
        elif file.endswith('.csv'):
            try:
                df = pd.read_csv(file, nrows=10)
                for _, row in df.iterrows():
                    data = row.to_dict()
                    author_id = data.get('author_id', '')
                    if author_id:
                        print(f"  Author ID: {author_id}")
            except Exception as e:
                print(f"Error reading CSV file {file}: {str(e)}")
    
    # Check book-author connections
    if book_author_files:
        print("\nSampling book-author connections:")
        file = book_author_files[0]
        if file.endswith('.json.gz'):
            with gzip.open(file, 'rt', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i >= 10:
                        break
                    try:
                        data = json.loads(line)
                        book_id = data.get('book_id', '')
                        author_id = data.get('author_id', '')
                        if book_id and author_id:
                            print(f"  Book {book_id} -> Author {author_id}")
                    except:
                        continue
        elif file.endswith('.csv'):
            try:
                df = pd.read_csv(file, nrows=10)
                for _, row in df.iterrows():
                    data = row.to_dict()
                    book_id = data.get('book_id', '')
                    author_id = data.get('author_id', '')
                    if book_id and author_id:
                        print(f"  Book {book_id} -> Author {author_id}")
            except Exception as e:
                print(f"Error reading CSV file {file}: {str(e)}")
    
    # Check for author IDs within book records
    if book_files:
        print("\nChecking for author IDs within book records:")
        file = book_files[0]
        if file.endswith('.json.gz'):
            with gzip.open(file, 'rt', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i >= 10:
                        break
                    try:
                        data = json.loads(line)
                        book_id = data.get('book_id', '')
                        authors = data.get('authors', [])
                        if book_id and authors:
                            print(f"  Book {book_id} has authors:", end=" ")
                            if isinstance(authors, list):
                                if authors and isinstance(authors[0], dict):
                                    author_info = [f"{a.get('author_id', '')}:{a.get('name', '')}" for a in authors[:3]]
                                    print(", ".join(author_info))
                                else:
                                    print(authors[:3])
                            else:
                                print(authors)
                    except:
                        continue
        elif file.endswith('.csv'):
            try:
                df = pd.read_csv(file, nrows=10)
                for _, row in df.iterrows():
                    data = row.to_dict()
                    book_id = data.get('book_id', '')
                    authors = data.get('authors', '')
                    if book_id and authors:
                        # Note: The CSV column may have a string representation of authors.
                        print(f"  Book {book_id} has authors: {authors}")
            except Exception as e:
                print(f"Error reading CSV file {file}: {str(e)}")

def main():
    files = get_dataset_files()
    print(f"Found {len(files)} dataset files")
    
    # Group files by type for better organization
    file_categories = {
        'book': [],
        'author': [],
        'user': [],
        'review': [],
        'interaction': [],
        'other': []
    }
    
    for file_path in files:
        filename_lower = os.path.basename(file_path).lower()
        if 'book' in filename_lower:
            file_categories['book'].append(file_path)
        elif 'author' in filename_lower:
            file_categories['author'].append(file_path)
        elif 'user' in filename_lower:
            file_categories['user'].append(file_path)
        elif 'review' in filename_lower:
            file_categories['review'].append(file_path)
        elif 'interaction' in filename_lower:
            file_categories['interaction'].append(file_path)
        else:
            file_categories['other'].append(file_path)
    
    # Print summary of file categories
    print("\nFile categories:")
    for category, files in file_categories.items():
        print(f"  {category}: {len(files)} files")
        for f in files:
            print(f"    - {os.path.basename(f)}")
    
    # Explore each file category (sample files)
    for category, files in file_categories.items():
        if not files:
            continue
        
        # Explore the first file in each category
        first_file = files[0]
        if first_file.endswith('.json.gz'):
            explore_file(first_file)
        elif first_file.endswith('.csv'):
            explore_csv_file(first_file)
        
        # If there are multiple files, also sample the second one to see differences
        if len(files) > 1:
            second_file = files[1]
            if second_file.endswith('.json.gz'):
                explore_file(second_file)
            elif second_file.endswith('.csv'):
                explore_csv_file(second_file)
    
    # Analyze ID connections
    find_id_connections()

if __name__ == "__main__":
    main()
