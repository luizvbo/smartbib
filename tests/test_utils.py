from smartbib.utils import id_bytes_to_str, id_str_to_bytes

def test_id_conversion():
    hash_str = '33b237709dbd53953a750355115b57ccb6690da1'
    assert id_bytes_to_str(id_str_to_bytes(hash_str)) == hash_str
