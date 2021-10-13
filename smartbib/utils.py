from typing import Union

def id_str_to_bytes(id_str: str) -> bytes:
    """Convert a 40 characters hash into a byte array.

    The conversion results in 160 bits of information (20-bytes array). Notice
    that this operation is reversible (using `id_bytes_to_str`).

    Args:
        id_str: Hash string containing 40 characters.

    Returns:
        bytes: The ID converted to bytes.
    """
    return int(id_str, 16).to_bytes(20, byteorder='big')


def id_bytes_to_str(id_bytes: bytes) -> str:
    """Convert a byte array from `id_int_to_str` back into string.

    Args:
        id_bytes: ID represented as a byte array.

    Returns:
        str: Hash as string.
    """
    return hex(int.from_bytes(id_bytes, byteorder='big'))[2:]


def chunks(lst: Union[list, tuple], n: int = 5_000):
    """Yield successive n-sized chunks from list.

    Args:
        lst: Input list/tuple used to generate the chunks
        n: Size of the chunks
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
