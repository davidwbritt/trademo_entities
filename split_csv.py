import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Iterator

import pandas as pd
from pandas.core.frame import DataFrame
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CSVProcessingError(Exception):
    """Custom exception for handling CSV processing-specific errors."""


def split_csv(
    input_file: str,
    output_folder: str,
    rows_per_output: int,
    delimiter: str = ",",
    quotechar: str = '"',
) -> None:
    """
    Split a CSV file into multiple smaller files with a fixed row count,
    with validations for required fields and type conversion for company_number.

    This function:
    1. Reads the input CSV in chunks to manage memory
    2. Ensures 'company_number' is treated as a string
    3. Filters each chunk to ensure non-null and non-empty values
       for 'company_number', 'jurisdiction_code', and 'name'
    4. Writes complete, validated chunks to output files
    5. Maintains a progress bar for processing status

    Args:
        input_file: Path to the input CSV file
        output_folder: Directory where output files will be saved
        rows_per_output: Number of rows to include in each output file
        delimiter: CSV delimiter character, defaults to comma
        quotechar: CSV quote character, defaults to double quote

    Raises:
        CSVProcessingError: If any error occurs during CSV processing
    """
    logger.info("Processing file: %s", input_file)
    logger.info("Splitting file into parts with %s rows each...", rows_per_output)

    try:
        if not os.path.exists(input_file):
            raise CSVProcessingError(f"Input file not found: {input_file}")

        input_file_base = os.path.splitext(os.path.basename(input_file))[0]
        current_date = datetime.now().strftime("%Y-%m-%d")

        os.makedirs(output_folder, exist_ok=True)

        # Count total rows for progress bar
        with open(input_file, "r", encoding="utf-8") as file:
            total_rows = sum(1 for _ in file)

        reader: Iterator[DataFrame] = pd.read_csv(
            input_file,
            chunksize=rows_per_output,
            low_memory=False,
            delimiter=delimiter,
            quotechar=quotechar,
        )

        chunk_index = 0
        with tqdm(total=total_rows, desc="Splitting file") as pbar:
            for chunk_index, chunk in enumerate(reader, 1):
                # Ensure 'company_number' is treated as a string
                if "company_number" in chunk.columns:
                    chunk["company_number"] = chunk["company_number"].astype(str)

                # Validation: Filter rows with non-null, non-empty values in required fields
                required_columns = ["company_number", "jurisdiction_code", "name"]
                for col in required_columns:
                    if col in chunk.columns:
                        chunk = chunk[chunk[col].notna() & (chunk[col] != "")]

                # Generate output filename
                output_file = os.path.join(
                    output_folder,
                    f"{input_file_base}_{current_date}_part{chunk_index:04d}.csv",
                )

                # Write validated chunk to output file
                chunk.to_csv(output_file, index=False, quoting=1)

                # Update progress bar
                pbar.update(len(chunk))

        logger.info(
            "File splitting complete. Source split into %s parts with %s rows in each.",
            chunk_index,
            rows_per_output,
        )

    except pd.errors.EmptyDataError as exc:
        raise CSVProcessingError(f"The input file is empty: {input_file}") from exc
    except pd.errors.ParserError as exc:
        raise CSVProcessingError(f"Failed to parse input file: {input_file}") from exc
    except IOError as exc:
        raise CSVProcessingError(
            f"IO error occurred while processing file: {input_file}"
        ) from exc
    except Exception as exc:
        logger.exception("An unexpected error occurred: %s", exc)
        raise CSVProcessingError(
            f"Unexpected error during CSV processing: {input_file}"
        ) from exc


def main() -> None:
    """
    Main entry point for the CSV splitting script.

    Parses command-line arguments and initiates the CSV splitting workflow.
    Handles any errors that occur during processing and sets appropriate exit codes.

    Command-line arguments:
        - input_file: Path to the input CSV file
        - output_folder: Directory for output files
        - --rows_per_output: Number of rows per output file (default: 500000)
        - --delimiter: CSV delimiter character (default: ',')
        - --quotechar: CSV quote character (default: '"')
        - --log_level: Logging level (default: INFO)
    """
    parser = argparse.ArgumentParser(
        description="Split a CSV file into smaller parts with fixed number of rows, with validations."
    )
    parser.add_argument("input_file", type=str, help="The input CSV file path.")
    parser.add_argument(
        "output_folder", type=str, help="The folder where output files will be saved."
    )
    parser.add_argument(
        "--rows_per_output",
        type=int,
        default=500000,
        help="Number of rows per output file (default: 500000).",
    )
    parser.add_argument(
        "--delimiter", type=str, default=",", help="CSV delimiter (default: ',')."
    )
    parser.add_argument(
        "--quotechar",
        type=str,
        default='"',
        help="CSV quote character (default: '\"').",
    )
    parser.add_argument(
        "--log_level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)",
    )

    args = parser.parse_args()

    logging.getLogger().setLevel(args.log_level)

    try:
        split_csv(
            args.input_file,
            args.output_folder,
            args.rows_per_output,
            args.delimiter,
            args.quotechar,
        )
    except CSVProcessingError as e:
        logging.error("CSV processing error: %s", e)
        sys.exit(1)
    except (OSError, IOError) as e:
        logging.error("File system error: %s", e)
        sys.exit(1)
    except ValueError as e:
        logging.error("Invalid value error: %s", e)
        sys.exit(1)
    # pylint: disable=broad-exception-caught
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()

