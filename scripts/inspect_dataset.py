from pathlib import Path
import pandas as pd


def main():
    project_root = Path(__file__).resolve().parent.parent

    csv_path = project_root / "datasets" / "raw" / "v2.csv"

    print("=" * 80)
    print("Reading sample...")
    print("=" * 80)

    df = pd.read_csv(csv_path, nrows=1000, low_memory=False)

    print("\nShape (sample):")
    print(df.shape)

    print("\nColumns:")
    for i, col in enumerate(df.columns, start=1):
        print(f"{i:02d}. {col}")

    print("\nData Types:")
    print(df.dtypes)

    print("\nMissing Values:")
    print(df.isna().sum())

    print("\nDuplicate Rows (sample):")
    print(df.duplicated().sum())

    print("\nFirst 5 Rows:")
    print(df.head())

    print("\nLast 5 Rows:")
    print(df.tail())

    print("\nMemory Usage (sample):")
    print(df.memory_usage(deep=True).sum() / 1024**2, "MB")

    print("=" * 80)

    print(df["accData"].head())
if __name__ == "__main__":
    main()