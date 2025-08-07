import uuid

from airport.model import TableFile, MergePlan
import os


class MergeConfiguration:
    timeout_s: int
    max_result_bytes: int
    iteration: int


configurations = [
    MergeConfiguration(timeout_s=10, max_result_bytes=40 * 1024 * 1024, iteration=1),
    MergeConfiguration(timeout_s=100, max_result_bytes=400 * 1024 * 1024, iteration=2),
    MergeConfiguration(timeout_s=1000, max_result_bytes=4000 * 1024 * 1024, iteration=3),
    MergeConfiguration(timeout_s=4000, max_result_bytes=4000 * 1024 * 1024, iteration=4),
]

class Merger:
    # list of merging tasks by folder by iteration
    merge_queues: dict[str, dict[int, list[MergePlan]]] = {}

    def get_iteration(self, path):
        info = os.path.basename(path).split(".")[1]
        if len(info) < 3 or not info[1].isdigit() or int(info[1]) < 1 or int(info[1]) > 4:
            return 1
        return int(info[1])

    @staticmethod
    def new_merge_plan(cls, directory: str, iteration: int) -> MergePlan:
        return MergePlan(
            to_file_path=os.path.join(directory, f"{uuid.uuid4()}.{iteration+1}.parquet"),
            iteration=iteration,
        )


    def add_file(self, path: str, table_file: TableFile):
        directory = os.path.dirname(path)
        if directory not in self.merge_queues:
            self.merge_queues[directory] = {}
        iteration = self.get_iteration(path)
        if iteration not in self.merge_queues[directory]:
            self.merge_queues[directory][iteration] = []

        merge_queue = self.merge_queues[directory][iteration]
        if len(merge_queue) == 0:
            merge_queue.append(self.new_merge_plan(directory, iteration))

        merge_plan = merge_queue[-1]
        if merge_plan.size_bytes + table_file.size_bytes > configurations[iteration-1].max_result_bytes or \
                len(merge_plan.from_file_paths) > 10:
            merge_queue.append(MergePlan())
            merge_plan = merge_queue[-1]
        merge_plan.from_file_paths.append(path)
        merge_plan.size_bytes += table_file.size_bytes
        merge_plan.from_table_files.append(table_file)

    def do_merge(self):
        pass


    def merge_one(self, iteration: int):
        for directory, iterations in self.merge_queues.items():
            if iterations not in iterations:
                continue
            if len(iterations[iteration]) == 0:
                continue
            iterations[iteration][0]



