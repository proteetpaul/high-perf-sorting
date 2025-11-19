import pandas as pd
import matplotlib.pyplot as plt
import os
import argparse
import sys
from io import StringIO
from matplotlib.dates import DateFormatter, AutoDateLocator 
from matplotlib.ticker import FuncFormatter

CONVERSION_FACTOR = 1024.0
UNIT_LABEL = 'GB/s'

def plot_memory_bandwidth(csv_filepath: str):
    """
    Reads the Intel PCM CSV data, calculates total utilized memory bandwidth 
    (Read + Write), and generates a time series plot.

    Args:
        csv_filepath (str): Path to the PCM output CSV file.
    """
    try:
        # Load the CSV file. 
        # The header spans two rows (0 and 1) due to the SKT/System groupings.
        df = pd.read_csv(csv_filepath, header=[0, 1])
        
        # --- Data Preprocessing ---
        
        # 1. Combine Date and Time columns into a single DateTime string
        # The columns are multi-indexed, so we access them using tuples.
        df['DateTime_str'] = df[('Unnamed: 0_level_0', 'Date')] + ' ' + df[('Unnamed: 1_level_0', 'Time')]
        
        # 2. Convert the combined string to a proper datetime object and set as index
        df['DateTime'] = pd.to_datetime(df['DateTime_str'])
        df = df.set_index('DateTime')
        
        # 3. Identify the relevant total system bandwidth columns
        # We use the 'System' group columns named 'Read' and 'Write'
        read_col_name = ('System', 'Read')
        write_col_name = ('System', 'Write')

        # 4. Clean the data and convert to float.
        # PCM output often includes padding spaces (including non-breaking spaces \xa0)
        # which prevent direct conversion. We strip all whitespace before converting.
        
        def clean_and_convert(series):
            """Strips whitespace (including \xa0) and converts column to float."""
            return series.astype(str).str.strip().astype(float)

        df[read_col_name] = clean_and_convert(df[read_col_name])
        df[write_col_name] = clean_and_convert(df[write_col_name])

        df[f'System Read Bandwidth ({UNIT_LABEL})'] = df[read_col_name] / CONVERSION_FACTOR
        df[f'System Write Bandwidth ({UNIT_LABEL})'] = df[write_col_name] / CONVERSION_FACTOR
        
        # --- Calculation ---
        
        # Calculate the total utilized memory bandwidth (Read + Write)
        df['Total Bandwidth (GB/s)'] = (df[read_col_name] + df[write_col_name]) / 1024
        
        # --- Plotting ---
        
        plt.style.use('seaborn-v0_8-darkgrid')
        
        # Create the figure and axis
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Plot the calculated total bandwidth
        ax.plot(
            df.index, 
            df['Total Bandwidth (GB/s)'], 
            label='Total Bandwidth (Read + Write)',
            color='#1f77b4',
            linewidth=2.5
        )
        
        # Add a second plot for individual Read/Write components for context
        ax.plot(
            df.index,
            df[f'System Read Bandwidth ({UNIT_LABEL})'],
            label='System Read Bandwidth',
            color='#ff7f0e',
            linestyle='--'
        )
        ax.plot(
            df.index,
            df[f'System Write Bandwidth ({UNIT_LABEL})'],
            label='System Write Bandwidth',
            color='#2ca02c',
            linestyle=':'
        )

        # Formatting the plot
        ax.set_title(
            'Time Series Analysis of Utilized Memory Bandwidth (System Total)', 
            fontsize=16, 
            fontweight='bold', 
            pad=20
        )

        date_form = DateFormatter('%H:%M:%S.%f')
        def custom_millisecond_formatter(x, pos):
            """Matplotlib FuncFormatter to format datetimes to HH:MM:SS.ms"""
            # Use the microsecond formatter to get the full string
            full_string = date_form(x, pos)
            
            if '.' in full_string:
                # Find the decimal point and return the string up to the 3rd digit after it.
                # decimal_index + 4 ensures we keep '.' and 3 digits (e.g., .123)
                decimal_index = full_string.find('.')
                return full_string[:decimal_index + 4]
            else:
                return full_string # Should not happen with this data, but included for robustness


        ax.set_xlabel('Time', fontsize=12)
        
        # ax.xaxis.set_major_formatter(date_form)
        ax.xaxis.set_major_formatter(FuncFormatter(custom_millisecond_formatter))
        ax.xaxis.set_major_locator(AutoDateLocator(maxticks=15))

        ax.set_ylabel('Bandwidth (GB/s)', fontsize=12)
        
        # Improve x-axis date formatting
        fig.autofmt_xdate(rotation=45)
        
        # Add legend and grid
        ax.legend(loc='upper right', frameon=True, shadow=True)
        ax.grid(True, linestyle='--', alpha=0.7)
        
        plt.tight_layout()
        plt.savefig("plot.png")

    except FileNotFoundError:
        print(f"Error: The file '{csv_filepath}' was not found.")
    except KeyError as e:
        print(f"Error: Missing expected column in the CSV. Check header names. Missing key: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generate a time-series plot of memory bandwidth from Intel PCM CSV output."
    )
    # Define the required positional argument for the file path
    parser.add_argument(
        'filepath',
        type=str,
        help='The path to the Intel PCM CSV file (e.g., /path/to/data.csv)'
    )
    
    args = parser.parse_args()
    
    # Call the plotting function with the provided file path
    plot_memory_bandwidth(args.filepath)