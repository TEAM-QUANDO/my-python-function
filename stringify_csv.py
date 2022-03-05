from io import StringIO
from collections import deque

def stringify_csv(data: str, keepHeader=True) -> str:
    """Given a CSV format, drop the first header line

    Args:
        data (str): CSV data
        keepHeader (bool, optional): Whether to drop the header or not. Defaults to True.

    Returns:
        str: CSV without header
    """
    def dropfirstline(f: StringIO, dropfirst=1, buffersize=100):
        f.seek(0)
        buffer = deque()
        tail_pos = 0

        # these next two loops assume the file has many thousands of
        # lines so we can safely drop and buffer the first few...
        for _ in range(dropfirst):
            f.readline()

        for _ in range(buffersize):
            buffer.append(f.readline())

        line = f.readline()
        while line:
            buffer.append(line)
            head_pos = f.tell()
            f.seek(tail_pos)
            tail_pos += f.write(buffer.popleft())
            f.seek(head_pos)
            line = f.readline()

        f.seek(tail_pos)
        # finally, clear out the buffer:
        while buffer:
            f.write(buffer.popleft())

        f.truncate()
    with StringIO(data) as f:
        if not keepHeader:
            dropfirstline(f)
            data = f.getvalue()
        else:
            data = f.read()
    return data
