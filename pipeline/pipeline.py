import collections
from queue import Queue

class Pipeline:
    def __init__(self, relations, task_ids):
        '''
        relations: relations.txt file path
        task_ids: task_ids.txt file path
        '''
        self.graph = self.build_graph(relations)
        self.ids = self.load_task(task_ids)

    # build graph as adjacent list
    def build_graph(self, relations):
        graph = collections.defaultdict(list)
        with open(relations) as f:
            for line in f:
                line = line.strip()
                if not line: continue
                fr, to = line.split("->") if "->" in line else (None, None)
                graph[to].append(fr)
        return graph

    # load task ids 
    def load_task(self, task_ids):
        ids = []
        with open(task_ids) as f:
            for line in f:
                line = line.strip()
                if not line: continue
                ids += line.split(",") if "," in line else []
        return ids

    # 1. ret1: use dfs to find all the tasks need to be done before goal, in topological order
    # 2. ret2: use dfs to find all the tasks already been done before start, in topological order
    # 3. ret: remove tasks in ret2 from ret1, but start should be included.
    def get_dependency(self, start, goal):
        if start not in self.ids or goal not in self.ids:
            return []

        color = {i:0 for i in self.ids}
        def dfs(i, ret):
            color[i] = 1
            for nei in self.graph[i]:
                if color[nei] == 0:
                    dfs(nei, ret)
            color[i] = 2
            ret.append(i)
        
        ret1 = []
        dfs(goal, ret1)
        color = {i:0 for i in self.ids}
        # print(ret1)

        ret2 = []
        dfs(start, ret2)
        # print(ret2)

        # remove tasks in ret2 from ret1
        ret = []
        for task in ret1:
            if task == start or task not in ret2:
                ret.append(task)

        return ret


if __name__ == '__main__':
    question = "question.txt"
    relations = "relations.txt"
    task_ids = "task_ids.txt"
    start, goal = None, None
    with open(question) as f:
        for line in f:
            line = line.strip()
            if not line: continue
            elif "starting task" in line:
                start = line.split(":")[1].strip()
            elif "goal task" in line:
                goal = line.split(":")[1].strip()
            else:
                break
    # print(start, goal, type(start), type(goal))
    pipeline = Pipeline(relations, task_ids)
    # print(pipeline.graph)
    d = pipeline.get_dependency(start, goal)
    print("d: ", d)

