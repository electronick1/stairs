import copy
import queue


class GraphItem:

    def __init__(self, pipeline_component, next=None):
        self.p_component = pipeline_component

        self.id = pipeline_component.id
        self.next = next if next else []

    def __copy__(self, **kwargs):
        """
        Special copy for graph, which copy all vertex objects inside.

        IMPORTANT:
        You should not use deepcopy for graph, because pipeline components
        has stepist key (stepist object) and it shouldn't be duplicated.

        """
        item = self.__class__(self.p_component,
                              next=[])

        for n in self.next:
            item.next.append(copy.copy(n))
        
        return item

    def __eq__(self, other):
        return self.p_component.id == \
               other.p_component.id

    def __str__(self):
        return str(self.id)

    def copy(self):
        return copy.copy(self)

    def add_next(self, base_item, copy=True):

        # it's important to create new Item instance,
        # because otherwise different graph items will have same 'next' values
        if copy:
            self.next.append(base_item.copy())
        else:
            self.next.append(base_item)


class PipelineGraph:
    """
    Graph which made specialy for Pipeline objects.
    This graph is actually a tree:
    (graph which don't have cycles and amount of edges = amount of items - 1).

    Some terms:
        Leave - graph item which don't have "next" items
        Level - the distance between root and some item
        Max Level - the depth of the Graph.


    """

    def __init__(self, root=None):
        self.root = root

    def __copy__(self):
        cls = self.__class__

        return cls(root=copy.copy(self.root))

    def __str__(self):
        result_items = []
        for item in dfs(self.root):
            result_items.append(str(item))

        return str(result_items)

    def get_root(self):
        """
        Return graph root. (The first added item to the graph)
        """
        return self.root

    def is_line_tree(self):
        """
        "Line tree" - is a tree which have only one leave.

        :return: True if graph (tree) has one leave only
        """

        return len(get_leaves(self)) == 1

    def extend_leaves(self, new_item):
        """
        Add new graph item to all defined level items.

        Looks like:

        If we have :

                (2) -> (4)
        (1) ->
                (3) -> (5)

        And we want to add (6) on all leaves

                (2) -> (4) -> (6.1)
        (1) ->
                (3) -> (5) -> (6.2)

        """

        if self.root is None:
            self.root = new_item.copy()
            return

        for item in get_leaves(self):
            if not item.next:
                item.add_next(new_item)

    def add_pipeline_component(self, pipeline_component):
        graph_item = GraphItem(pipeline_component)

        leaves_affected = get_leaves(self)
        self.extend_leaves(graph_item)

        return leaves_affected

    def add_graphs_on_leaves(self, *new_graphs):
        """
        TODO: documentation
        """

        current_leaves = get_leaves(self)

        if not current_leaves:
            raise RuntimeError("You can't add multiple graphs to empty graph")

        for item in current_leaves:
            for new_graph in new_graphs:
                self.add_graph_root_to_item(item, new_graph.root)

    def add_graph_on_leaves(self, graph):
        # syntax sugar function
        # mockup under add_graphS_on_leaves

        self.add_graphs_on_leaves(*[graph])

    def add_graph_root_to_item(self, parrent_item, new_graph_root):

        parrent_item.add_next(new_graph_root)

        for item in new_graph_root.next:
            self.add_graph_root_to_item(new_graph_root, item)


# graph utils:

def get_leaves(graph):
    if graph.root is None:
        return []

    return get_leaves_graph_item(graph.root)


def get_leaves_graph_item(g_item):
    return [item for item in dfs(g_item) if not item.next]


def dfs(current_item, backward=False):
    """
    Go through all graph in recursion way.
    See https://en.wikipedia.org/wiki/Depth-first_search for more info

    :param current_item: Item which we should start from
    :param backward: The way how we should return results

    :yield: graph items in dfs way.
    """
    if not backward:
        yield current_item

    for next_item in current_item.next:
        for item in dfs(next_item, backward=backward):
            yield item

    if backward:
        yield current_item


def dfs_by_multiple(item1, item2, backward=False):
    """
    Same like general dfs. But we iteration only by similar vertex from both
    graphs.
    """

    if item1 != item2:
        return

    if not backward:
        yield item1, item2

    for next_item1 in item1.next:
        for next_item2 in item2.next:
            if next_item1 != next_item2:
                continue

            for item1, item2 in dfs_by_multiple(next_item1, next_item2,
                                                backward=backward):
                yield item1, item2

    if backward:
        yield  item1, item2


def bfs(current_item):
    """
    Go through all graph using queue (FIFO)
    See https://en.wikipedia.org/wiki/Breadth-first_search for more info

    :yield: graph items in bfs way
    """
    q = queue.Queue()
    q.put(current_item)

    while not q.empty():
        g_item = q.get()
        yield g_item

        for next_item in g_item.next:
            q.put(next_item)


def concatenate_sequentially(graph1, graph2):
    """
    Combine two different graphs with same root in "line" tree.
    Both graphs should be "line" tree. (one vertex - one child)

    If we have two following graphs:

    1)

    (1) -> (2) -> (3) -> (4)

    2)

    (1) -> (2) -> (5) - (6)

    We should merge them into:

    (1) -> (2) -> (3) -> (4) -> (5) -> (6)

    And add data transformation function to min parent leave:

    (1) -> (2) -> (3) -> (4) -> (5) -> (6)
            \                   ^
              -  - - - - - - - /
                data from (2)

    :return graph item which need to be extend by transformation function
    """

    if not graph1.is_line_tree() or not graph2.is_line_tree():
        pass

    graph = merge_graphs_by_roots(graph1, graph2)
    leaves = get_leaves(graph)

    # if we have only one leave -> g2 is subset of g1
    if len(leaves) == 1:
        return graph, None, None

    # if both graphs are inline, we should always have 2 leaves after merging
    if len(leaves) != 2:
        raise RuntimeError("Something wrong with graph after concatinate, "
                           "looks like it's not a 'line' tree ")

    for g_item in dfs(graph.root):
        if len(g_item.next) == 2:
            left_item = g_item.next[0]
            right_item = g_item.next[1]

            left_leave = get_leaves_graph_item(left_item)[0]

            # add connection to next
            left_leave.add_next(right_item)

            # remove connection to right item
            g_item.next = [left_item]

            return graph, g_item, right_item


def merge_graphs_by_roots(graph1, graph2):
    """
    Populate graph1 by graph2 items. Consider only none-equal elements.
    """

    if graph1.root != graph2.root:
        raise RuntimeError("Graphs has different roots")

    for g1_item, g2_item in dfs_by_multiple(graph1.root, graph2.root):
        different_items_from_g2 = []
        # Extract all items from g2 which different from g1 on g1_item level.
        for next_item_g2 in g2_item.next:
            g1_has_item_from_g2 = False

            for next_item_g1 in g1_item.next:
                if next_item_g1 == next_item_g2:
                    g1_has_item_from_g2 = True

            if not g1_has_item_from_g2:
                different_items_from_g2.append(next_item_g2.copy())

        g1_item.next.extend(different_items_from_g2)

    return graph1
