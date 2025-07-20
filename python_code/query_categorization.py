import csv
import re
import sys
import os

def score_query(query):
    score = 0
    
    if not re.search(r'SELECT \*', query, re.IGNORECASE):
        score += 1
    if re.search(r'LIMIT', query, re.IGNORECASE):
        score += 1
    if re.search(r'WHERE', query, re.IGNORECASE):
        score += 2
    if re.search(r'HAVING', query, re.IGNORECASE):
        score += 2
    if re.search(r'ORDER BY', query, re.IGNORECASE):
        score += 1
    joins = len(re.findall(r'JOIN', query, re.IGNORECASE))
    score += joins * 3
    if re.search(r'GROUP BY', query, re.IGNORECASE):
        score += 2
    aggregations = len(re.findall(r'(COUNT|SUM|AVG|MAX|MIN)\s*\(', query, re.IGNORECASE))
    score += min(aggregations, 1) * 2 + max(aggregations - 1, 0)
    subqueries = len(re.findall(r'\(SELECT', query, re.IGNORECASE))
    score += subqueries * 3
    if re.search(r'WITH', query, re.IGNORECASE):
        score += 5
    if re.search(r'(UNION|INTERSECT|EXCEPT)', query, re.IGNORECASE):
        score += 5
    if re.search(r'OVER\s*\(', query, re.IGNORECASE):
        score += 6
    if re.search(r'(PIVOT|UNPIVOT)', query, re.IGNORECASE):
        score += 6

    return score

def categorize_query(score):
    if score <= 2:
        return "Basic Exploratory"
    elif score <= 6:
        return "Focused Exploratory"
    elif score <= 13:
        return "Analytical"
    else:
        return "Complex Analytical"

def explain_score(query):
    explanations = []
    if not re.search(r'SELECT \*', query, re.IGNORECASE):
        explanations.append("SELECT with specific columns (1)")
    if re.search(r'LIMIT', query, re.IGNORECASE):
        explanations.append("LIMIT clause (1)")
    if re.search(r'WHERE', query, re.IGNORECASE):
        explanations.append("WHERE clause (2)")
    if re.search(r'HAVING', query, re.IGNORECASE):
        explanations.append("HAVING clause (2)")
    if re.search(r'ORDER BY', query, re.IGNORECASE):
        explanations.append("ORDER BY (1)")
    joins = len(re.findall(r'JOIN', query, re.IGNORECASE))
    if joins > 0:
        explanations.append(f"JOIns ({joins * 3})")
    if re.search(r'GROUP BY', query, re.IGNORECASE):
        explanations.append("GROUP BY (2)")
    aggregations = len(re.findall(r'(COUNT|SUM|AVG|MAX|MIN)\s*\(', query, re.IGNORECASE))
    if aggregations > 0:
        explanations.append(f"Aggregations ({min(aggregations, 1) * 2 + max(aggregations - 1, 0)})")
    subqueries = len(re.findall(r'\(SELECT', query, re.IGNORECASE))
    if subqueries > 0:
        explanations.append(f"Subqueries ({subqueries * 3})")
    if re.search(r'WITH', query, re.IGNORECASE):
        explanations.append("Common Table Expressions (5)")
    if re.search(r'(UNION|INTERSECT|EXCEPT)', query, re.IGNORECASE):
        explanations.append("Set operations (5)")
    if re.search(r'OVER\s*\(', query, re.IGNORECASE):
        explanations.append("Window functions (6)")
    if re.search(r'(PIVOT|UNPIVOT)', query, re.IGNORECASE):
        explanations.append("PIVOT/UNPIVOT (6)")
    return ", ".join(explanations)

def process_queries(input_file, output_file):
    with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        
        # UPDATED: Add new columns to existing fieldnames from table capture output
        fieldnames = reader.fieldnames + ['score', 'category', 'explanation']
        
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            # UPDATED: Use snake_case column names to match table capture output
            query = row['query']
            completeness = row.get('completeness', '1')
            
            if completeness == '1':
                score = score_query(query)
                category = categorize_query(score)
                explanation = explain_score(query)
            else:
                score = 'N/A'
                category = 'Invalid/Incomplete'
                explanation = 'Query marked as incomplete'
            
            # UPDATED: Use snake_case for new columns
            row['score'] = score
            row['category'] = category
            row['explanation'] = explanation
            
            writer.writerow(row)

    print(f"Processing complete. Results saved to {output_file}")

if __name__ == "__main__":
    # UPDATED: Set paths for new folder structure
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    curated_output_dir = os.path.join(project_root, "curated_output")
    
    if len(sys.argv) != 2:
        print("Usage: python query_categorization.py <input_filename>")
        print("Example: python query_categorization.py query_table_captured.csv")
        sys.exit(1)
    
    input_filename = sys.argv[1]
    if not input_filename.endswith('.csv'):
        input_filename += '.csv'
    
    # UPDATED: Read from curated_output folder
    input_file = os.path.join(curated_output_dir, input_filename)
    output_file = os.path.join(curated_output_dir, "categorized_queries.csv")
    
    if not os.path.exists(input_file):
        print(f"File not found: {input_file}")
        print(f"Make sure you've run the table capture step first!")
        sys.exit(1)
    
    process_queries(input_file, output_file)