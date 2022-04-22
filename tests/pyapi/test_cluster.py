from hyperqueue.cluster import LocalCluster


def test_create_cluster():
    with LocalCluster():
        pass
