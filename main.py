"""
Terminal CLI for the Data Query Agent system.
Usage:  python main.py <path-to-csv-or-xlsx>
"""
import sys
import os

from graph import DataQuerySystem


def print_banner():
    print("=" * 60)
    print("  DATA QUERY AGENT  -  LangGraph Multi-Agent System")
    print("=" * 60)
    print()


def main():
    print_banner()

    # --- Get file path ---
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = input("Enter path to CSV or Excel file: ").strip().strip('"')

    if not os.path.isfile(file_path):
        print(f"Error: file not found -> {file_path}")
        sys.exit(1)

    system = DataQuerySystem()

    # --- Ingestion + Schema ---
    print(f"\n[1/2] Loading file: {file_path}")
    print("      Ingesting into SQLite and mapping schema...\n")

    try:
        schema_summary = system.ingest(file_path)
    except Exception as e:
        print(f"Error during ingestion: {e}")
        sys.exit(1)

    print("-" * 60)
    print("SCHEMA DESCRIPTION:")
    print("-" * 60)
    print(schema_summary)
    print("-" * 60)
    print()

    # --- Query loop ---
    print("[2/2] Ready! Ask questions about your data.")
    print('      Type "exit" or "quit" to stop.\n')

    while True:
        try:
            question = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not question:
            continue
        if question.lower() in ("exit", "quit", "q"):
            print("Goodbye!")
            break

        print("\nThinking...\n")
        try:
            answer = system.ask(question)
            print(f"Agent: {answer}\n")
        except Exception as e:
            print(f"Error: {e}\n")


if __name__ == "__main__":
    main()
