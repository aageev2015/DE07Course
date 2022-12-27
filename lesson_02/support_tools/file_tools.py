import os


def guarantee_folder_exists(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path)


def has_path_sub_folder(parent_path: str, testing_path: str) -> bool:
    normalized = os.path.normcase(testing_path)
    first_folder_is_raw = normalized.startswith(parent_path) or normalized.startswith(os.sep + parent_path)
    normalized_len = len(normalized)
    right_folder_sep_idx = normalized.rfind(os.sep, 0, normalized_len - 1)
    has_sub_folder = right_folder_sep_idx not in [0, normalized_len - 2]

    return first_folder_is_raw and has_sub_folder
