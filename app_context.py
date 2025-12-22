
from collections import deque
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="templates")

RECENT_SEARCHES = deque(maxlen=5)
EXTERNAL_SEARCHES = deque(maxlen=15)
