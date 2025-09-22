#!/usr/bin/env python3
# Compare transactions between poly_nyc_dem_nom_zm_trades.csv (accurate) and cleaned_trades.csv
# Match by timestamp and transaction hash, check buy/sell and yes/no accuracy

import pandas as pd
from typing import Dict, List, Tuple

def load_and_prepare_data():
    """Load both CSV files and prepare for comparison"""
    print("Loading CSV files...")
    
    # Load the accurate reference data
    ref_df = pd.read_csv("poly_nyc_dem_nom_zm_trades.csv")
    print(f"Reference data: {len(ref_df)} rows")
    
    # Load the cleaned data to compare
    cleaned_df = pd.read_csv("cleaned_trades.csv")
    print(f"Cleaned data: {len(cleaned_df)} rows")
    
    # Convert timestamp to int for comparison
    ref_df['timestamp'] = pd.to_numeric(ref_df['timestamp'], errors='coerce').astype('Int64')
    cleaned_df['timestamp'] = pd.to_numeric(cleaned_df['timestamp'], errors='coerce').astype('Int64')
    
    return ref_df, cleaned_df

def find_matching_transactions(ref_df: pd.DataFrame, cleaned_df: pd.DataFrame) -> List[Dict]:
    """Find transactions where ANY ref row matches ANY cleaned row for the same transaction hash"""
    transaction_groups = []
    
    print("\nFinding matching transactions...")
    
    # Group reference data by transaction hash
    ref_groups = ref_df.groupby('transactionHash')
    
    for tx_hash, ref_group in ref_groups:
        # Find matching rows in cleaned data for this transaction
        matching_cleaned = cleaned_df[cleaned_df['transactionHash'] == tx_hash]
        
        if len(matching_cleaned) > 0:
            # Check if ANY ref row matches ANY cleaned row
            sides_match = False
            outcomes_match = False
            price_match = False
            
            # Get all unique values from both groups
            ref_sides = set(ref_group['side'].dropna().str.upper())
            cleaned_sides = set(matching_cleaned['side'].dropna().str.upper())
            ref_outcomes = set(ref_group['outcome'].dropna().str.upper())
            cleaned_outcomes = set(matching_cleaned['outcome'].dropna().str.upper())
            
            # Check if there's any overlap
            if ref_sides & cleaned_sides:  # Any common sides
                sides_match = True
            
            if ref_outcomes & cleaned_outcomes:  # Any common outcomes
                outcomes_match = True
            
            # Check price overlap (within 1% tolerance)
            ref_prices = ref_group['price'].dropna()
            cleaned_prices = matching_cleaned['price'].dropna()
            
            if len(ref_prices) > 0 and len(cleaned_prices) > 0:
                try:
                    ref_prices_float = ref_prices.astype(float)
                    cleaned_prices_float = cleaned_prices.astype(float)
                    
                    # Check if any ref price is close to any cleaned price
                    for ref_price in ref_prices_float:
                        for cleaned_price in cleaned_prices_float:
                            if ref_price != 0 and abs(ref_price - cleaned_price) / ref_price < 0.01:
                                price_match = True
                                break
                        if price_match:
                            break
                except (ValueError, TypeError, ZeroDivisionError):
                    pass
            
            transaction_groups.append({
                'transactionHash': tx_hash,
                'ref_count': len(ref_group),
                'cleaned_count': len(matching_cleaned),
                'ref_sides': ref_sides,
                'cleaned_sides': cleaned_sides,
                'ref_outcomes': ref_outcomes,
                'cleaned_outcomes': cleaned_outcomes,
                'ref_prices': ref_prices.tolist(),
                'cleaned_prices': cleaned_prices.tolist(),
                'sides_match': sides_match,
                'outcomes_match': outcomes_match,
                'price_match': price_match,
                'ref_group': ref_group,
                'cleaned_group': matching_cleaned
            })
    
    print(f"Found {len(transaction_groups)} matching transaction groups")
    return transaction_groups

def analyze_matches(transaction_groups: List[Dict]):
    """Analyze the transaction groups for accuracy"""
    if not transaction_groups:
        print("No matching transaction groups found!")
        return
    
    print(f"\nAnalyzing {len(transaction_groups)} transaction groups...")
    
    # Count matches and mismatches
    sides_correct = sum(1 for group in transaction_groups if group['sides_match'])
    sides_incorrect = len(transaction_groups) - sides_correct
    
    outcomes_correct = sum(1 for group in transaction_groups if group['outcomes_match'])
    outcomes_incorrect = len(transaction_groups) - outcomes_correct
    
    prices_correct = sum(1 for group in transaction_groups if group['price_match'])
    prices_incorrect = len(transaction_groups) - prices_correct
    
    # Collect incorrect cases
    incorrect_cases = []
    
    for group in transaction_groups:
        issues = []
        if not group['sides_match']:
            issues.append(f"sides: ref={group['ref_sides']}, cleaned={group['cleaned_sides']}")
        if not group['outcomes_match']:
            issues.append(f"outcomes: ref={group['ref_outcomes']}, cleaned={group['cleaned_outcomes']}")
        if not group['price_match']:
            issues.append(f"prices: ref={group['ref_prices'][:3]}, cleaned={group['cleaned_prices'][:3]}")
        
        if issues:
            incorrect_cases.append({
                'tx': group['transactionHash'],
                'ref_count': group['ref_count'],
                'cleaned_count': group['cleaned_count'],
                'issues': issues
            })
    
    # Print results
    print(f"\n=== TRANSACTION GROUP ACCURACY ===")
    print(f"Total transaction groups: {len(transaction_groups)}")
    
    print(f"\n=== SIDE (Buy/Sell) ACCURACY ===")
    print(f"Correct groups: {sides_correct}")
    print(f"Incorrect groups: {sides_incorrect}")
    if len(transaction_groups) > 0:
        accuracy = sides_correct / len(transaction_groups) * 100
        print(f"Accuracy: {accuracy:.1f}%")
    
    print(f"\n=== OUTCOME (Yes/No) ACCURACY ===")
    print(f"Correct groups: {outcomes_correct}")
    print(f"Incorrect groups: {outcomes_incorrect}")
    if len(transaction_groups) > 0:
        accuracy = outcomes_correct / len(transaction_groups) * 100
        print(f"Accuracy: {accuracy:.1f}%")
    
    print(f"\n=== PRICE ACCURACY ===")
    print(f"Correct groups (within 1%): {prices_correct}")
    print(f"Incorrect groups: {prices_incorrect}")
    if len(transaction_groups) > 0:
        accuracy = prices_correct / len(transaction_groups) * 100
        print(f"Accuracy: {accuracy:.1f}%")
    
    # Show some incorrect cases
    if incorrect_cases:
        print(f"\n=== INCORRECT TRANSACTION GROUPS ===")
        for i, case in enumerate(incorrect_cases[:10]):  # Show first 10
            print(f"{i+1}. TX: {case['tx'][:20]}... (ref: {case['ref_count']} rows, cleaned: {case['cleaned_count']} rows)")
            for issue in case['issues']:
                print(f"   - {issue}")
        if len(incorrect_cases) > 10:
            print(f"... and {len(incorrect_cases) - 10} more incorrect groups")
    
    # Show summary stats
    print(f"\n=== SUMMARY STATS ===")
    ref_total_rows = sum(group['ref_count'] for group in transaction_groups)
    cleaned_total_rows = sum(group['cleaned_count'] for group in transaction_groups)
    print(f"Total reference rows: {ref_total_rows}")
    print(f"Total cleaned rows: {cleaned_total_rows}")
    print(f"Row count difference: {cleaned_total_rows - ref_total_rows}")

def main():
    print("=== Transaction Comparison Analysis ===")
    
    try:
        ref_df, cleaned_df = load_and_prepare_data()
        matches = find_matching_transactions(ref_df, cleaned_df)
        analyze_matches(matches)
        
    except FileNotFoundError as e:
        print(f"Error: Could not find file - {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
