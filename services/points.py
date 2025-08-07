import time
from typing import Tuple, Any, Dict, List

class Point:
    def __init__(self, key: bytes, fields: bytes, timestamp: bytes, time: int):
        self.key = key
        self.fields = fields
        self.ts = timestamp
        self.time = time
        self.cached_name = None
        self.cached_tags = None
        self.cached_fields = None

    def Name(self) -> str:
        if self.cached_name is None:
            self.cached_name = self._parse_name()
        return self.cached_name

    def Tags(self) -> Dict[str, str]:
        if self.cached_tags is None:
            self.cached_tags = self._parse_tags()
        return self.cached_tags

    def Fields(self) -> Dict[str, Any]:
        if self.cached_fields is None:
            self.cached_fields = self._parse_fields()
        return self.cached_fields

    def _parse_name(self) -> str:
        name, _ = scan_to(self.key, 0, b',')
        return unescape_measurement(name).decode('utf-8')

    def _parse_tags(self) -> Dict[str, str]:
        tags = {}
        _, rest = scan_to(self.key, 0, b',')
        if not rest:
            return tags

        while rest:
            key, rest = scan_to(rest, 0, b'=')
            value, rest = scan_to(rest, 0, b',')
            tags[unescape_tag(key).decode()] = unescape_tag(value).decode()

        return tags

    def _parse_fields(self) -> Dict[str, Any]:
        fields = {}
        rest = self.fields
        while rest:
            key, rest = scan_to(rest, 0, b'=')
            if not rest:
                raise ValueError("invalid field format")

            value, rest = scan_field_value(rest)
            fields[key.decode()] = parse_field_value(value)

            if rest.startswith(b','):
                rest = rest[1:]

        return fields

def scan_to(buf: bytes, i: int, stop: bytes) -> Tuple[bytes, bytes]:
    start = i
    while i < len(buf):
        if buf[i:i+1] == stop and (i == 0 or buf[i-1:i] != b'\\'):
            return buf[start:i], buf[i+1:]
        i += 1
    return buf[start:], b''

def scan_field_value(buf: bytes) -> Tuple[bytes, bytes]:
    quoted = False
    i = 0
    for i, char in enumerate(buf):
        if char == ord('\\') and i + 1 < len(buf) and buf[i+1] in (ord('"'), ord('\\')):
            i += 1
            continue
        if char == ord('"'):
            quoted = not quoted
        elif char == ord(',') and not quoted:
            return buf[:i], buf[i:]
    return buf, b''

def parse_field_value(value: bytes) -> Any:
    if value.startswith(b'"') and value.endswith(b'"'):
        return unescape_string_field(value[1:-1].decode('utf-8'))
    elif value == b't':
        return True
    elif value == b'f':
        return False
    elif value.endswith(b'i'):
        return int(value[:-1])
    elif b'.' in value or b'e' in value or b'E' in value:
        return float(value)
    else:
        return int(value)

def unescape_tag(tag: bytes) -> bytes:
    if ord('\\') not in tag:
        return tag

    tag_escape_codes = [
        (b',', b'\\,'),
        (b' ', b'\\ '),
        (b'=', b'\\='),
    ]

    for code in tag_escape_codes:
        tag = tag.replace(code[1], code[0])

    return tag

def unescape_string_field(value: str) -> str:
    return value.replace('\\"', '"').replace('\\\\', '\\')


def parse_points(buf: bytes) -> List[Point]:
    return parse_points_with_precision(buf, time.time_ns(), "n")

def parse_points_with_precision(buf: bytes, default_time: int, precision: str) -> List[Point]:
    points = []
    failed = []
    pos = 0

    while pos < len(buf):
        pos, block = scan_line(buf, pos)
        pos += 1

        if len(block) == 0:
            continue

        start = skip_whitespace(block, 0)

        # If line is all whitespace, just skip it
        if start >= len(block):
            continue

        # lines which start with '#' are comments
        if block[start] == ord('#'):
            continue

        # strip the newline if one is present
        if block[-1] == ord('\n'):
            block = block[:-1]

        pt = parse_point(block[start:], default_time, precision)
        points.append(pt)

    return points

def scan_line(buf: bytes, i: int) -> Tuple[int, bytes]:
    start = i
    quoted = False
    fields = False

    # tracks how many '=' and commas we've seen
    # this duplicates some of the functionality in scanFields
    equals = 0
    commas = 0

    while i < len(buf):
        # skip past escaped characters
        if buf[i] == ord('\\') and i + 2 < len(buf):
            i += 2
            continue

        if buf[i] == ord(' '):
            fields = True

        if fields:
            if not quoted and buf[i] == ord('='):
                i += 1
                equals += 1
                continue
            elif not quoted and buf[i] == ord(','):
                i += 1
                commas += 1
                continue
            elif buf[i] == ord('"') and equals > commas:
                i += 1
                quoted = not quoted
                continue

        if buf[i] == ord('\n') and not quoted:
            break

        i += 1

    return i, buf[start:i]

def skip_whitespace(buf: bytes, i: int) -> int:
    while i < len(buf):
        if buf[i] not in (ord(' '), ord('\t'), 0):
            break
        i += 1
    return i

def parse_point(buf: bytes, default_time: int, precision: str) -> Point:
    # scan the first block which is measurement[,tag1=value1,tag2=value2...]
    pos, key = scan_key(buf, 0)
    if not key:
        raise ValueError("missing measurement")

    if len(key) > MAX_KEY_LENGTH:
        raise ValueError(f"max key length exceeded: {len(key)} > {MAX_KEY_LENGTH}")

    # scan the second block which is field1=value1[,field2=value2,...]
    pos, fields = scan_fields(buf, pos)
    if not fields:
        raise ValueError("missing fields")

    max_key_err = check_field_key_lengths(key, fields)
    if max_key_err:
        raise ValueError(max_key_err)

    # scan the last block which is an optional integer timestamp
    pos, ts = scan_time(buf, pos)

    pt = Point(key, fields, ts, 0)

    if not ts:
        pt.time = default_time
        set_precision(pt, precision)
    else:
        ts_value = int(ts)
        pt.time = safe_calc_time(ts_value, precision)

    # Determine if there are illegal non-whitespace characters after the timestamp block
    while pos < len(buf):
        if buf[pos] != ord(' '):
            raise ValueError("invalid point")
        pos += 1

    return pt

def scan_key(buf: bytes, i: int) -> Tuple[int, bytes]:
    start = skip_whitespace(buf, i)
    i = start

    # First scan the Point's measurement
    state, i, err = scan_measurement(buf, i)
    if err:
        raise ValueError(err)

    # Optionally scan tags if needed
    if state == TAG_KEY_STATE:
        i, commas, indices, err = scan_tags(buf, i)
        if err:
            raise ValueError(err)

        # Check for duplicate tags
        if commas > 1:
            tags = [buf[indices[j]:indices[j+1]-1] for j in range(commas-1)]
            if has_duplicate_tags(tags):
                raise ValueError("duplicate tags")

    key = buf[start:i]
    # Unescape the measurement name in the key
    measurement_end = key.index(b',') if b',' in key else len(key)
    unescaped_measurement = unescape_measurement(key[:measurement_end])
    key = unescaped_measurement + key[measurement_end:]

    return i, key


def scan_fields(buf: bytes, i: int) -> Tuple[int, bytes]:
    start = skip_whitespace(buf, i)
    i = start
    quoted = False
    equals = 0
    commas = 0

    while i < len(buf):
        if buf[i] == ord('\\') and i + 1 < len(buf):
            i += 2
            continue

        if buf[i] == ord('"') and equals > commas:
            quoted = not quoted
            i += 1
            continue

        if buf[i] == ord('=') and not quoted:
            equals += 1
            if i == start or buf[i-1] == ord(' ') or buf[i-1] == ord(','):
                raise ValueError("missing field key")
            if i + 1 >= len(buf):
                raise ValueError("missing field value")
            if buf[i+1] == ord(',') or buf[i+1] == ord(' '):
                raise ValueError("missing field value")

        if buf[i] == ord(',') and not quoted:
            commas += 1

        if buf[i] == ord(' ') and not quoted:
            break

        i += 1

    if quoted:
        raise ValueError("unbalanced quotes")

    if equals == 0 or commas != equals - 1:
        raise ValueError("invalid field format")

    return i, buf[start:i]

def scan_time(buf: bytes, i: int) -> Tuple[int, bytes]:
    start = skip_whitespace(buf, i)
    i = start

    while i < len(buf):
        if buf[i] == ord('\n') or buf[i] == ord(' '):
            break

        if i == start and buf[i] == ord('-'):
            i += 1
            continue

        if not (ord('0') <= buf[i] <= ord('9')):
            raise ValueError("bad timestamp")

        i += 1

    return i, buf[start:i]

def check_field_key_lengths(key: bytes, fields: bytes) -> str:
    def walk_fields(fields: bytes):
        i = 0
        while i < len(fields):
            j = fields.index(b'=', i)
            field_key = fields[i:j]
            i = fields.index(b',', j) if b',' in fields[j:] else len(fields)
            yield field_key
            i += 1  # skip the comma

    for field_key in walk_fields(fields):
        if series_key_size(key, field_key) > MAX_KEY_LENGTH:
            return f"max key length exceeded: {series_key_size(key, field_key)} > {MAX_KEY_LENGTH}"
    return ""

def series_key_size(key: bytes, field: bytes) -> int:
    # 4 is the length of the tsm1.fieldKeySeparator constant
    return len(key) + 4 + len(field)

def set_precision(pt: Point, precision: str):
    if precision == "n":
        return
    elif precision == "u":
        pt.time = pt.time // 1000 * 1000
    elif precision == "ms":
        pt.time = pt.time // 1000000 * 1000000
    elif precision == "s":
        pt.time = pt.time // 1000000000 * 1000000000
    elif precision == "m":
        pt.time = pt.time // 60000000000 * 60000000000
    elif precision == "h":
        pt.time = pt.time // 3600000000000 * 3600000000000

def safe_calc_time(ts: int, precision: str) -> int:
    multiplier = {
        "n": 1,
        "u": 1000,
        "ms": 1000000,
        "s": 1000000000,
        "m": 60000000000,
        "h": 3600000000000
    }
    if precision not in multiplier:
        raise ValueError(f"Invalid precision: {precision}")
    return ts * multiplier[precision]

# Constants
MAX_KEY_LENGTH = 65535
TAG_KEY_STATE = 1


from typing import Tuple

# Constants
TAG_KEY_STATE = 1
FIELD_STATE = 2

def scan_measurement(buf: bytes, i: int) -> Tuple[int, int, str]:
    # Check first byte of measurement, anything except a comma is fine.
    # It can't be a space, since whitespace is stripped prior to this
    # function call.
    if i >= len(buf) or buf[i] == ord(','):
        return -1, i, "missing measurement"

    while True:
        i += 1
        if i >= len(buf):
            # cpu
            return -1, i, "missing fields"

        if buf[i-1] == ord('\\'):
            # Skip character (it's escaped).
            continue

        # Unescaped comma; move onto scanning the tags.
        if buf[i] == ord(','):
            return TAG_KEY_STATE, i + 1, ""

        # Unescaped space; move onto scanning the fields.
        if buf[i] == ord(' '):
            # cpu value=1.0
            return FIELD_STATE, i, ""

    # This line should never be reached, but we'll include it for completeness
    return -1, i, "unexpected end of measurement"

# Helper function to unescape measurement names
def unescape_measurement(measurement: bytes) -> bytes:
    if ord('\\') not in measurement:
        return measurement

    # Define escape codes (similar to Go version)
    measurement_escape_codes = [
        (b',', b'\\,'),
        (b' ', b'\\ '),
    ]

    for code in measurement_escape_codes:
        measurement = measurement.replace(code[1], code[0])

    return measurement




def scan_tags(buf: bytes, i: int) -> Tuple[int, int, List[int], str]:
    commas = 0
    indices = [0] * 100  # Arbitrary large size, similar to Go version
    state = TAG_KEY_STATE
    err = ""

    while True:
        if state == TAG_KEY_STATE:
            # Grow our indices slice if we have too many tags
            if commas >= len(indices):
                indices.extend([0] * len(indices))

            indices[commas] = i
            commas += 1

            i, err = scan_tag_key(buf, i)
            if err:
                return i, commas, indices, err
            state = TAG_VALUE_STATE  # tag value always follows a tag key

        elif state == TAG_VALUE_STATE:
            state, i, err = scan_tag_value(buf, i)
            if err:
                return i, commas, indices, err

        elif state == FIELD_STATE:
            # Grow our indices slice if we had exactly enough tags to fill it
            if commas >= len(indices):
                indices.append(0)
            indices[commas] = i + 1
            return i, commas, indices[:commas+1], ""

        else:
            return i, commas, indices[:commas], "invalid state"

def scan_tag_key(buf: bytes, i: int) -> Tuple[int, str]:
    # First character of the key
    if i >= len(buf) or buf[i] in (ord(' '), ord(','), ord('=')):
        return i, "missing tag key"

    # Examine each character in the tag key until we hit an unescaped
    # equals (the tag value), or we hit an error (i.e., unescaped
    # space or comma)
    while True:
        i += 1

        # Either we reached the end of the buffer or we hit an
        # unescaped comma or space
        if i >= len(buf) or ((buf[i] in (ord(' '), ord(','))) and buf[i-1] != ord('\\')):
            return i, "missing tag value"

        if buf[i] == ord('=') and buf[i-1] != ord('\\'):
            return i + 1, ""

def scan_tag_value(buf: bytes, i: int) -> Tuple[int, int, str]:
    # Tag value cannot be empty
    if i >= len(buf) or buf[i] in (ord(','), ord(' ')):
        return -1, i, "missing tag value"

    # Examine each character in the tag value until we hit an unescaped
    # comma (move onto next tag key), an unescaped space (move onto
    # fields), or we error out
    while True:
        i += 1
        if i >= len(buf):
            return -1, i, "missing fields"

        # An unescaped equals sign is an invalid tag value
        if buf[i] == ord('=') and buf[i-1] != ord('\\'):
            return -1, i, "invalid tag format"

        if buf[i] == ord(',') and buf[i-1] != ord('\\'):
            return TAG_KEY_STATE, i + 1, ""

        if buf[i] == ord(' ') and buf[i-1] != ord('\\'):
            return FIELD_STATE, i, ""

# Constants (if not already defined)
TAG_KEY_STATE = 1
TAG_VALUE_STATE = 2
FIELD_STATE = 3

def has_duplicate_tags(tags: List[bytes]) -> bool:
    seen = set()
    for tag in tags:
        key = tag.split(b'=')[0]
        if key in seen:
            return True
        seen.add(key)
    return False
