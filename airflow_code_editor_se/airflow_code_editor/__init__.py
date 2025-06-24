# import sys
# from typing import List
#
# def deduplicate_python_path() -> None:
#     """
#     Our python path may contain duplicates that will lead to discovering our adyen plugin multiple times.
#     To avoid that, we deduplicate the python path first while remaining an ordering based on first occurrences.
#     """
#     new_list: List[str] = []
#     for item in sys.path:
#         if item not in new_list:
#             new_list.append(item)
#     sys.path = new_list
#
# deduplicate_python_path()
