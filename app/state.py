from collections import deque

# 最近の検索条件（メモリ上）
RECENT_SEARCHES = deque(maxlen=5)
EXTERNAL_SEARCHES = deque(maxlen=15)
