import pandas as pd

def split_dataset(input_file, train_file, test_file, train_size=500):
    # Load the dataset
    data = pd.read_csv(input_file)

    # Ensure reproducibility
    data = data.sample(frac=1, random_state=42).reset_index(drop=True)

    # Split the dataset
    train_data = data.iloc[:train_size]
    test_data = data.iloc[train_size:]

    # Save the train and test datasets
    train_data.to_csv(train_file, index=False)
    test_data.to_csv(test_file, index=False)

if __name__ == "__main__":
    input_path = "./data/examples.csv"
    train_path = "./data/train.csv"
    test_path = "./data/test.csv"
    split_dataset(input_path, train_path, test_path)
