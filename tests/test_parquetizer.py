from smartbib.parquetizer import read_json_gzip


def test_read_json_gzip():
    df_data = read_json_gzip('tests/_resources/s2-corpus-sample.gz')
    assert df_data.shape == (10, 13)
    assert len(df_data.index.names) == 3
