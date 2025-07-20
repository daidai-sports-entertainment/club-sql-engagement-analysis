import csv
import sys
import os
from collections import Counter

def analyze_usage(input_file, output_file):
    # Dictionary to store table usage counts
    table_usage = Counter()
    
    # UPDATED: Track additional metrics for enhanced analysis
    environment_usage = Counter()
    club_table_usage = {}
    complexity_by_table = {}

    # Read the input CSV and count table usage
    with open(input_file, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            club = row.get('club', '')
            data_env = row.get('data_environment', '')
            category = row.get('category', '')
            
            # Count environment usage
            if data_env:
                environment_usage[data_env] += 1
            
            # UPDATED: Use new snake_case column names and expanded table columns
            table_columns = ['first_table_used', 'second_table_used', 'third_table_used', 
                           'fourth_table_used', 'fifth_table_used', 'sixth_table_used']
            
            for column in table_columns:
                table = row.get(column, '').strip()
                if table:  # Only count non-empty table names
                    table_usage[table] += 1
                    
                    # Track club usage
                    if club not in club_table_usage:
                        club_table_usage[club] = Counter()
                    club_table_usage[club][table] += 1
                    
                    # Track complexity by table
                    if table not in complexity_by_table:
                        complexity_by_table[table] = []
                    if category:
                        complexity_by_table[table].append(category)

    # Sort tables by usage count in descending order
    sorted_table_usage = sorted(table_usage.items(), key=lambda x: x[1], reverse=True)

    # UPDATED: Create comprehensive analysis output
    create_detailed_analysis(output_file, sorted_table_usage, environment_usage, 
                           club_table_usage, complexity_by_table)
    
    print(f"Analysis complete. Results written to {output_file}")
    return sorted_table_usage, environment_usage, club_table_usage

def create_detailed_analysis(output_file, sorted_table_usage, environment_usage, 
                           club_table_usage, complexity_by_table):
    """Create a comprehensive analysis with multiple sheets/sections"""
    
    base_name = output_file.replace('.csv', '')
    
    # 1. Overall table usage ranking
    with open(f"{base_name}_table_ranking.csv", 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['table_name', 'usage_count', 'percentage_of_total'])
        
        total_usage = sum(count for _, count in sorted_table_usage)
        for table, count in sorted_table_usage:
            percentage = (count / total_usage) * 100 if total_usage > 0 else 0
            writer.writerow([table, count, f"{percentage:.2f}%"])
    
    # 2. Environment usage analysis
    with open(f"{base_name}_environment_analysis.csv", 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['data_environment', 'query_count', 'percentage'])
        
        total_env = sum(environment_usage.values())
        for env, count in environment_usage.most_common():
            percentage = (count / total_env) * 100 if total_env > 0 else 0
            writer.writerow([env, count, f"{percentage:.2f}%"])
    
    # 3. Club engagement analysis
    with open(f"{base_name}_club_engagement.csv", 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['club', 'total_table_references', 'unique_tables_used', 'engagement_score'])
        
        club_stats = []
        for club, table_counter in club_table_usage.items():
            total_refs = sum(table_counter.values())
            unique_tables = len(table_counter)
            # Simple engagement score: total refs + bonus for table diversity
            engagement_score = total_refs + (unique_tables * 2)
            club_stats.append((club, total_refs, unique_tables, engagement_score))
        
        # Sort by engagement score descending
        club_stats.sort(key=lambda x: x[3], reverse=True)
        for club, total_refs, unique_tables, engagement_score in club_stats:
            writer.writerow([club, total_refs, unique_tables, engagement_score])
    
    # 4. Table complexity analysis
    with open(f"{base_name}_table_complexity.csv", 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['table_name', 'usage_count', 'avg_complexity_level', 'complexity_distribution'])
        
        complexity_levels = {'Basic Exploratory': 1, 'Focused Exploratory': 2, 
                           'Analytical': 3, 'Complex Analytical': 4}
        
        for table, count in sorted_table_usage[:20]:  # Top 20 tables
            if table in complexity_by_table:
                categories = complexity_by_table[table]
                if categories:
                    # Calculate average complexity
                    complexity_scores = [complexity_levels.get(cat, 0) for cat in categories]
                    avg_complexity = sum(complexity_scores) / len(complexity_scores)
                    
                    # Get distribution
                    cat_counter = Counter(categories)
                    distribution = ', '.join([f"{cat}:{cnt}" for cat, cnt in cat_counter.most_common()])
                    
                    writer.writerow([table, count, f"{avg_complexity:.2f}", distribution])

def print_summary(sorted_table_usage, environment_usage, n=10):
    """Print a quick summary to console"""
    print(f"\n📊 SUMMARY REPORT")
    print(f"=" * 50)
    
    print(f"\n🏆 Top {n} Most Used Tables:")
    for i, (table, count) in enumerate(sorted_table_usage[:n], 1):
        print(f"{i:2d}. {table}: {count} uses")
    
    print(f"\n🌐 Data Environment Usage:")
    total_env = sum(environment_usage.values())
    for env, count in environment_usage.most_common():
        percentage = (count / total_env) * 100 if total_env > 0 else 0
        print(f"    {env}: {count} queries ({percentage:.1f}%)")

if __name__ == "__main__":
    # UPDATED: Set paths for new folder structure
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    curated_output_dir = os.path.join(project_root, "curated_output")
    
    if len(sys.argv) != 2:
        print("Usage: python table_usage.py <input_filename>")
        print("Example: python table_usage.py categorized_queries.csv")
        sys.exit(1)
    
    input_filename = sys.argv[1]
    if not input_filename.endswith('.csv'):
        input_filename += '.csv'
    
    # UPDATED: Read from and write to curated_output folder
    input_file = os.path.join(curated_output_dir, input_filename)
    output_file = os.path.join(curated_output_dir, "usage_analysis.csv")
    
    if not os.path.exists(input_file):
        print(f"File not found: {input_file}")
        print(f"Make sure you've run the categorization step first!")
        sys.exit(1)
    
    # Run the analysis
    sorted_usage, env_usage, club_usage = analyze_usage(input_file, output_file)
    
    # Print summary to console
    print_summary(sorted_usage, env_usage)