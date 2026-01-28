import os
import secrets
import sys
import timeit
from random import getrandbits

sys.path.insert(1, os.path.join(sys.path[0], ".."))


width = 16

def random_string_old(width: int = 16):
    num_chars = ((width + 1) >> 1) << 3  # Convert length to number of bits
    rand_bytes = getrandbits(num_chars)  # Generate random bytes
    return "00" + hex(rand_bytes)[2:][-width:]

def random_string_format(width: int = 16):
    num_chars = ((width + 1) >> 1) << 3
    rand_bytes = getrandbits(num_chars)
    return "{:0>{}}".format(hex(rand_bytes)[2:], width)

def random_string_fstring(width: int = 16):
    num_chars = ((width + 1) >> 1) << 3
    rand_bytes = getrandbits(num_chars)
    return f"{hex(rand_bytes)[2:]:0>{width}}"

def random_string_secrets(width: int = 16):
    """
    Generate a random string of a given width.
    """
    num_bytes = (width + 1) // 2  # Convert width to number of bytes
    rand_hex = secrets.token_hex(num_bytes)  # Generate a hex string
    return rand_hex[:width]  # Truncate to desired length

# Test each function with a timer
old_time = timeit.timeit(lambda: random_string_old(width), number=1000000)
format_time = timeit.timeit(lambda: random_string_format(width), number=1000000)
fstring_time = timeit.timeit(lambda: random_string_fstring(width), number=1000000)
secrets_time = timeit.timeit(lambda: random_string_secrets(width), number=1000000)
nothing_time = timeit.timeit(hex(0), number=1000000)

print(f"Old method took: {old_time} seconds")
print(f"Format method took: {format_time} seconds")
print(f"F-string method took: {fstring_time} seconds")
print(f"Secret method took: {secrets_time} seconds")
print(f"Nothing method took: {nothing_time} seconds")

