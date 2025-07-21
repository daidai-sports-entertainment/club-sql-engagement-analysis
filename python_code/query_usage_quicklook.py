#!/usr/bin/env python3
"""
NFL Query Analytics Pipeline - Step 3: Table Usage Analysis

This script performs essential data aggregation for SQL analysis, creating two key data cuts:
1. Club-level summary: engagement metrics and table preferences by NFL team
2. Table-level summary: popularity and environment classification of data tables

The output provides clean, normalized data ready for downstream SQL analytics to answer
business questions about team engagement, platform migration, and table optimization.
"""

import csv
import sys
import os
from collections import Counter, defaultdict

def analyze_usage(input_file, output_file):
    """
    Main analysis function that processes categorized query data and generates
    club-level and table-level summaries.
    
    Args:
        input_file (str): Path to categorized_queries.csv from step 2
        output_file (str): Base path for output files (will generate _club_summary.csv and _table_summary.csv)
    """
    # Initialize data structures for tracking club and table metrics
    club_data = defaultdict(lambda: {
        'legacy_tables': set(),           # Unique legacy tables used by club
        'gridiron_tables': set(),         # Unique gridiron tables used by club  
        'legacy_table_counts': Counter(), # Usage frequency of each legacy table
        'gridiron_table_counts': Counter(), # Usage frequency of each gridiron table
        'scores': []                      # All query complexity scores for averaging
    })
    
    table_data = defaultdict(lambda: {
        'unique_clubs': set(),    # Which clubs use this table
        'query_appearances': 0,   # Total times table appears in queries
        'environment': 'unknown'  # legacy/gridiron classification
    })

    # Process input data row by row
    with open(input_file, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            club = row.get('club', '').strip()
            data_env = row.get('data_environment', '').strip()
            score = row.get('score', '')
            
            # Convert score to numeric (handle 'N/A' for incomplete queries)
            try:
                numeric_score = float(score) if score != 'N/A' else 0
            except:
                numeric_score = 0
            
            # Track query scores for club-level analytics
            if club:
                club_data[club]['scores'].append(numeric_score)
            
            # Process all table columns (supports up to 6 tables per query)
            table_columns = ['first_table_used', 'second_table_used', 'third_table_used', 
                           'fourth_table_used', 'fifth_table_used', 'sixth_table_used']
            
            for column in table_columns:
                table = row.get(column, '').strip()
                if table:
                    # Update table-level metrics
                    table_data[table]['unique_clubs'].add(club)
                    table_data[table]['query_appearances'] += 1
                    
                    # Classify table environment (legacy AWS vs new Gridiron)
                    if data_env in ['legacy', 'gridiron']:
                        table_data[table]['environment'] = data_env
                    
                    # Update club-level table usage tracking
                    if club and data_env in ['legacy', 'gridiron']:
                        if data_env == 'legacy':
                            club_data[club]['legacy_tables'].add(table)
                            club_data[club]['legacy_table_counts'][table] += 1
                        elif data_env == 'gridiron':
                            club_data[club]['gridiron_tables'].add(table)
                            club_data[club]['gridiron_table_counts'][table] += 1

    # Generate outputs
    base_name = output_file.replace('.csv', '')
    
    # 1st Data Cut: Club Analysis
    create_club_analysis(f"{base_name}_club_summary.csv", club_data)
    
    # 2nd Data Cut: Table Analysis  
    create_table_analysis(f"{base_name}_table_summary.csv", table_data)
    
    print(f"Analysis complete. Generated:")
    print(f"  - {base_name}_club_summary.csv")
    print(f"  - {base_name}_table_summary.csv")

def create_club_analysis(output_file, club_data):
    """
    Generate club-level engagement summary.
    
    Creates metrics for each NFL team including:
    - Platform usage (legacy vs gridiron table counts)
    - Query complexity (total and average scores)  
    - Table preferences (most frequently used tables by platform)
    
    Args:
        output_file (str): Path for club summary CSV output
        club_data (dict): Processed club metrics from analyze_usage()
    """
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['club', 'unique_legacy_tables', 'unique_gridiron_tables', 
                        'total_query_score', 'avg_query_score', 'most_used_legacy_table', 'most_used_gridiron_table'])
        
        for club, data in club_data.items():
            unique_legacy = len(data['legacy_tables'])
            unique_gridiron = len(data['gridiron_tables'])
            
            # Calculate engagement metrics
            # Total score = overall engagement volume
            # Average score = analytical sophistication level
            total_score = sum(data['scores']) if data['scores'] else 0
            avg_score = total_score / len(data['scores']) if data['scores'] else 0
            
            # Identify most frequently used tables by platform
            most_legacy = data['legacy_table_counts'].most_common(1)
            most_gridiron = data['gridiron_table_counts'].most_common(1)
            
            most_legacy_table = most_legacy[0][0] if most_legacy else ''
            most_gridiron_table = most_gridiron[0][0] if most_gridiron else ''
            
            writer.writerow([club, unique_legacy, unique_gridiron, f"{total_score:.2f}", f"{avg_score:.2f}", 
                           most_legacy_table, most_gridiron_table])

def create_table_analysis(output_file, table_data):
    """
    Generate table-level popularity and adoption summary.
    
    Creates metrics for each data table including:
    - Platform classification (legacy vs gridiron)
    - Adoption breadth (number of unique clubs using it)
    - Usage frequency (total query appearances)
    
    Args:
        output_file (str): Path for table summary CSV output  
        table_data (dict): Processed table metrics from analyze_usage()
    """
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['table_name', 'data_environment', 'unique_clubs_using', 'total_query_appearances'])
        
        # Sort by popularity (most queried tables first)
        sorted_tables = sorted(table_data.items(), 
                             key=lambda x: x[1]['query_appearances'], 
                             reverse=True)
        
        for table_name, data in sorted_tables:
            unique_clubs = len(data['unique_clubs'])
            appearances = data['query_appearances']
            environment = data['environment']
            
            writer.writerow([table_name, environment, unique_clubs, appearances])

def print_quick_summary(club_file, table_file):
    """
    Display console summary of key findings for immediate validation.
    
    Shows top performers in engagement and table popularity to help verify
    the data processing worked correctly before moving to SQL analysis.
    
    Args:
        club_file (str): Path to club summary CSV
        table_file (str): Path to table summary CSV
    """
    print(f"\n📊 QUICK SUMMARY")
    print(f"=" * 40)
    
    # Top clubs by average score
    club_scores = []
    with open(club_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                score = float(row['avg_query_score'])
                club_scores.append((row['club'], score))
            except:
                continue
    
    if club_scores:
        club_scores.sort(key=lambda x: x[1], reverse=True)
        print(f"\n🏆 Top 5 Clubs by Avg Query Complexity:")
        for i, (club, score) in enumerate(club_scores[:5], 1):
            print(f"{i}. {club}: {score:.2f}")
    
    # Top tables
    print(f"\n📋 Top 5 Most Popular Tables:")
    with open(table_file, 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, 1):
            if i <= 5:
                print(f"{i}. {row['table_name']}: {row['total_query_appearances']} uses")

if __name__ == "__main__":
    """
    Command line interface for table usage analysis.
    
    Usage:
        python table_usage.py categorized_queries.csv
        
    Input: 
        categorized_queries.csv (from step 2 - query categorization)
        
    Output:
        usage_analysis_club_summary.csv - Club engagement metrics
        usage_analysis_table_summary.csv - Table popularity metrics
    """
    # Configure paths relative to project structure
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    curated_output_dir = os.path.join(project_root, "curated_output")
    
    # Validate command line arguments
    if len(sys.argv) != 2:
        print("Usage: python table_usage.py <input_filename>")
        print("Example: python table_usage.py categorized_queries.csv")
        print("\nThis script generates essential data cuts for SQL analysis:")
        print("  1. Club-level engagement summary")
        print("  2. Table-level popularity summary")
        sys.exit(1)
    
    # Process input filename
    input_filename = sys.argv[1]
    if not input_filename.endswith('.csv'):
        input_filename += '.csv'
    
    input_file = os.path.join(curated_output_dir, input_filename)
    output_file = os.path.join(curated_output_dir, "usage_analysis.csv")
    
    # Validate input file exists
    if not os.path.exists(input_file):
        print(f"Error: File not found: {input_file}")
        print(f"Make sure you've run the categorization step first!")
        print(f"Expected location: {curated_output_dir}")
        sys.exit(1)
    
    # Execute analysis pipeline
    print(f"🔄 Processing {input_filename}...")
    analyze_usage(input_file, output_file)
    
    # Display validation summary
    base_name = output_file.replace('.csv', '')
    print_quick_summary(f"{base_name}_club_summary.csv", f"{base_name}_table_summary.csv")
    print(f"\n✅ Ready for SQL analysis!")