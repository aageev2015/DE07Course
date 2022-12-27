import os


def guarantee_folder_exists(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path)

