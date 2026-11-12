from dht.bucket import KBucket, K


def test_add_peer():
    bucket = KBucket()
    bucket.add_peer(10)
    assert 10 in bucket.get_peers()


def test_duplicate_peer():
    bucket = KBucket()
    bucket.add_peer(10)
    bucket.add_peer(10)
    assert bucket.size() == 1


def test_remove_peer():
    bucket = KBucket()
    bucket.add_peer(10)
    bucket.remove_peer(10)
    assert 10 not in bucket.get_peers()


def test_bucket_size():
    bucket = KBucket()

    for i in range(5):
        bucket.add_peer(i)

    assert bucket.size() == 5


def test_bucket_capacity():
    bucket = KBucket()

    for i in range(K):
        bucket.add_peer(i)

    assert bucket.size() == K


def test_full_bucket_returns_oldest_peer():
    bucket = KBucket()

    for i in range(K):
        bucket.add_peer(i)

    result = bucket.add_peer(100)

    assert result == 0
    assert bucket.size() == K


def test_existing_peer_moves_to_end():
    bucket = KBucket()

    bucket.add_peer(1)
    bucket.add_peer(2)
    bucket.add_peer(3)

    bucket.add_peer(1)

    assert bucket.get_peers() == [2, 3, 1]

