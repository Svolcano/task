import random
from pprint import pprint
import string
from queue import PriorityQueue

# 模拟 in addr
def gen_in_addr():
    return random.sample(range(10**2), 25)

# 模拟 out addr
def gen_out_addr():
    return random.sample(range(10**2), 25)

# 获取关键信息
def wash_row(row):
    temp_set = set()
    in_addr = row['in']
    out_addr = row['out']
    for a in in_addr:
        temp_set.add(a)
    for a in out_addr:
        temp_set.add(a)
    return row['index'], temp_set

# 产生模拟数据
def gen_sample(all_row_count):
    data = []
    for i in range(all_row_count):
        data.append({'index': i, 'in': gen_in_addr(), 'out': gen_out_addr()})
    return data

# 优先级队列插入完成排序， addr set 越长的约靠前， 为贪心准备数据
class Item():
    def __init__(self, index, row):
        self.priority = len(row)
        self.addr_set = row
        self.index = index
    # 越多数据越先出
    def __lt__(self, other):
        return self.priority > other.priority

    def __gt__(self, other):
        return self.priority < other.priority

    def __le__(self, other):
        return self.priority >= other.priority

    def __ge__(self, other):
        return self.priority <= other.priority

    def __str__(self):
        return "{} {} {}".format(self.priority, self.index, self.addr_set)


# 准备数据
all_addr_set = set()
# 来 50 条数据
all_row_count = 50
src = gen_sample(all_row_count)
priority_queue = PriorityQueue()

# 排序， 按照地址最多到最少的顺序， 由长到短。
for row in src:
    index, row_a_set = wash_row(row)
    item = Item(index, row_a_set)
    priority_queue.put(item)
    all_addr_set = all_addr_set | row_a_set

# 知道总计有多少 不同的 addr 即可， del 释放set 资源
total_addr_count = len(all_addr_set)
del all_addr_set

print(total_addr_count)


# 记录排好序的数据
# 因为queue 是单次遍历的
# 可以用 db 的排序功能，解决内存的问题
sorted_items = []
while not priority_queue.empty():
    a = priority_queue.get()
    print(a)
    sorted_items.append(a)

# 计算前的初始化
temp_set = sorted_items[0].addr_set
chose_indexs = [sorted_items[0].index]
del sorted_items[0]
finish = False

# 计算
while True:
    # 找到交集最少的， 地址最多的 贪心选择
    guard = total_addr_count
    minimum_index = 0
    chose_set = None
    minimum_len = 0
    c = 0
    while c < len(sorted_items):
        item = sorted_items[c]
        index = item.index
        addr_set = item.addr_set
        set_len = item.priority
        # 取交集
        join_set = temp_set & addr_set
        join_set_len = len(join_set)
        ratio = join_set_len / set_len
        # 已经被完全包含， 丢弃。
        if set_len == join_set_len:
            # duplicate
            del sorted_items[c]
            continue
        # 如果没有任何交集，直接选择
        elif ratio == 0 and set_len > (minimum_len * (1-guard)):
            # not included yet
            temp_set = temp_set | addr_set
            chose_indexs.append(index)
            if len(temp_set) == total_addr_count:
                finish = True
                break
            del sorted_items[c]
            continue
        # 如果有交集，那就找到交集最小的一个
        elif ratio < guard:
            guard = join_set_len/set_len
            minimum_len = set_len
            minimum_index = index
            chose_set = addr_set
        c += 1
    # 如果找到了就合并更新结果集
    if chose_set is not None:
        chose_indexs.append(minimum_index)
        temp_set = temp_set | chose_set
        if len(temp_set) == total_addr_count:
            finish = True
    # 找到所有的set 就结束
    if finish:
        break

# 输出结果， 选取的数据的 index
print("\n\n 选择的数据：", chose_indexs, '共计多少个', len(chose_indexs))

# 找到了所有的
print("\n\n 找到的地址个数：", len(temp_set), total_addr_count)

# 打印所有地址
print("\n\n 所有地址：", temp_set)
