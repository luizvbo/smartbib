from typing import Tuple


def id_str_to_int(id_str: str) -> Tuple[int]:
    """Convert a 40 characters hash into integers.

    The conversion results in 160 bits of information (20 bytes), which are
    split into 2 int32 and 1 int16 (both unsigned).

    Args:
        id_str: Hash string containing 40 characters.

    Returns:
        List[int]: List containing 2 int32 and 1 int16 (all unsigned).
    """
    return tuple(
        int(hex_str, 16) for hex_str in
        (id_str[:16], id_str[16:32], id_str[32:])
    )

def id_int_to_str(big_int_1: int, big_int_2: int, int_: int) -> str:
    """Convert the 3 integers generated from `id_int_to_str` back to string.

    Args:
        big_int_1: Input int32
        big_int_2: Input int32
        int_: Input int16

    Returns:
        str: Hash as string.
    """
    return (
        hex(big_int_1)[2:] + hex(big_int_2)[2:] + hex(int_)[2:]
    )

def id_str_to_bytes(id_str: str) -> bytes:
    return int(id_str, 16).to_bytes(20, byteorder='big')

def id_bytes_to_str(id_bytes: bytes) -> str:
    return hex(int.from_bytes(id_bytes, byteorder='big'))[2:]


def chunks(lst, n=5_000):
    """Yield successive n-sized chunks from list."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

