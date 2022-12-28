import os


def guarantee_folder_exists(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path)


def has_path_sub_folder(parent_path: str, testing_path: str) -> bool:
    """
    Check testing path:
            contains path parent as root
        and has at least one sub folder
    :param parent_path: parent path
    :param testing_path: testing path
    :return:
    """
    if parent_path == '':
        return True
    norm_parent = remove_leading_slash(os.path.normpath(parent_path)) + os.sep
    norm_testing = remove_leading_slash(os.path.normpath(testing_path))
    first_folder_is_matched = norm_testing.startswith(norm_parent)
    has_sub_folder = len(norm_testing) > len(norm_parent)
    return first_folder_is_matched and has_sub_folder


def logical_to_physical_file_path(physical_root: str, logical_path: str, logical_file_name: str) -> str:
    """
    convert logical file path to physical full path
    :param physical_root: physical local path. Must be result of os.path.normpath
    :param logical_path: provided by client logical path. Environment independent
    :param logical_file_name: file name
    """
    norm_phy_root = os.path.normpath(physical_root)
    if norm_phy_root == '.':
        norm_phy_root = ''
    relative_logic_path = remove_leading_slash(logical_path)
    norm_logic_path = os.path.normpath(relative_logic_path)
    if norm_logic_path == '.':
        norm_logic_path = ''
    result = os.path.join(*[norm_phy_root, norm_logic_path, logical_file_name])
    return result


def remove_leading_slash(path: str):
    if path == '':
        return path
    return path[1:] if path[0] in ["/", "\\"] else path
