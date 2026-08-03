from pathlib import Path
import shutil

import kagglehub


def main():
    project_root = Path(__file__).resolve().parent.parent

    destination = project_root / "datasets" / "raw"
    destination.mkdir(parents=True, exist_ok=True)

    print("Downloading dataset from Kaggle...")

    dataset_path = Path(
        kagglehub.dataset_download(
            "yunlevin/levin-vehicle-telematics"
        )
    )

    print(f"Downloaded to cache: {dataset_path}")

    for file in dataset_path.iterdir():
        if file.is_file():
            shutil.copy2(file, destination / file.name)

    print(f"\nDataset copied to:\n{destination}")


if __name__ == "__main__":
    main()