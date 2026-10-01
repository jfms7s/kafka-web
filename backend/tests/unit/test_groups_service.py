import pytest
from confluent_kafka import KafkaError

from kafka_web.errors import (
    BrokerError,
    Conflict,
    Forbidden,
    KafkaTimeout,
    NotFound,
    ValidationFailed,
)
from kafka_web.services.groups import (
    GroupDetailView,
    GroupSummary,
    MemberView,
    OffsetView,
    create_group,
    delete_group,
    describe_group,
    list_groups,
    reset_offsets,
    resolve_targets,
)
from tests.fakes import (
    FakeGroupAdmin,
    group_description,
    group_listing,
    kafka_exc,
    member,
)


def committed_at(offset: int, partitions: int = 2, topic: str = "orders") -> dict:
    return {(topic, p): offset for p in range(partitions)}


# --- list --------------------------------------------------------------------------------------


def test_list_groups_sorted_with_lowercase_state_and_type() -> None:
    admin = FakeGroupAdmin(
        listings=[
            group_listing("zeta", "STABLE", "CONSUMER"),
            group_listing("Alpha", "EMPTY", "CLASSIC"),
            group_listing("mid", None, None),
        ]
    )

    assert list_groups(admin) == [
        GroupSummary("Alpha", "empty", "classic", False),
        GroupSummary("mid", "unknown", "unknown", False),
        GroupSummary("zeta", "stable", "consumer", False),
    ]


def test_list_groups_filter_is_a_case_insensitive_substring() -> None:
    admin = FakeGroupAdmin(
        listings=[group_listing("Billing-Service"), group_listing("orders"), group_listing("bill")]
    )

    assert [g.group_id for g in list_groups(admin, "BILL")] == ["Billing-Service", "bill"]
    assert list_groups(admin, "nope") == []


def test_list_groups_passes_the_timeout_to_broker_and_future() -> None:
    admin = FakeGroupAdmin(listings=[group_listing("g")])

    list_groups(admin, timeout=3.0)

    assert admin.kwargs_seen == [{"request_timeout": 3.0}]


def test_list_groups_surfaces_listing_errors_instead_of_a_partial_list() -> None:
    admin = FakeGroupAdmin(
        listings=[group_listing("g")], list_errors=[kafka_exc(KafkaError._TRANSPORT)]
    )

    with pytest.raises(BrokerError) as caught:
        list_groups(admin)
    assert caught.value.code == "broker_unreachable"


# --- describe ----------------------------------------------------------------------------------


def test_describe_computes_lag_per_partition() -> None:
    admin = FakeGroupAdmin(
        descriptions={
            "g": group_description(
                "g",
                "STABLE",
                members=[member("m1", "client-a", "/10.0.0.1", [("orders", 0), ("orders", 1)])],
            )
        },
        committed={"g": {("orders", 1): 4, ("orders", 0): 7}},
        latest={("orders", 0): 10, ("orders", 1): 4},
    )

    assert describe_group(admin, "g") == GroupDetailView(
        group_id="g",
        state="stable",
        type="classic",
        members=[MemberView("m1", "client-a", "/10.0.0.1", [("orders", 0), ("orders", 1)])],
        offsets=[
            OffsetView("orders", 0, committed=7, end=10, lag=3),
            OffsetView("orders", 1, committed=4, end=4, lag=0),
        ],
    )


def test_describe_lag_is_floored_at_zero_when_committed_is_past_the_end() -> None:
    admin = FakeGroupAdmin(
        descriptions={"g": group_description("g")},
        committed={"g": {("orders", 0): 12}},
        latest={("orders", 0): 10},
    )

    assert describe_group(admin, "g").offsets == [OffsetView("orders", 0, 12, 10, 0)]


def test_describe_unknown_end_offset_leaves_lag_unknown() -> None:
    # The topic was deleted: the per-partition lookup fails with UNKNOWN_TOPIC_OR_PART.
    admin = FakeGroupAdmin(
        descriptions={"g": group_description("g")},
        committed={"g": {("gone", 0): 5, ("orders", 0): 1}},
        latest={("orders", 0): 2},
    )
    admin.list_offsets_errors[("gone", 0)] = kafka_exc(KafkaError.UNKNOWN_TOPIC_OR_PART)

    assert describe_group(admin, "g").offsets == [
        OffsetView("gone", 0, committed=5, end=None, lag=None),
        OffsetView("orders", 0, committed=1, end=2, lag=1),
    ]


def test_describe_uncommitted_partition_has_no_committed_offset_or_lag() -> None:
    admin = FakeGroupAdmin(
        descriptions={"g": group_description("g")},
        committed={"g": {("orders", 0): -1001}},
        latest={("orders", 0): 9},
    )

    assert describe_group(admin, "g").offsets == [OffsetView("orders", 0, None, 9, None)]


def test_describe_other_end_offset_failures_are_not_swallowed() -> None:
    admin = FakeGroupAdmin(
        descriptions={"g": group_description("g")},
        committed={"g": {("orders", 0): 1}},
    )
    admin.list_offsets_errors[("orders", 0)] = kafka_exc(KafkaError._TRANSPORT)

    with pytest.raises(BrokerError):
        describe_group(admin, "g")


def test_describe_member_without_an_assignment_and_consumer_protocol_groups() -> None:
    admin = FakeGroupAdmin(
        descriptions={
            "g": group_description(
                "g", "STABLE", type="CONSUMER", members=[member("m", "c", "h", None)]
            )
        }
    )

    detail = describe_group(admin, "g")

    assert detail.type == "consumer"
    assert detail.members == [MemberView("m", "c", "h", [])]
    assert detail.offsets == []


def test_describe_unknown_group_is_not_found_for_dead_state() -> None:
    with pytest.raises(NotFound) as caught:
        describe_group(FakeGroupAdmin(), "ghost")
    assert caught.value.code == "group_not_found"


def test_describe_unknown_group_is_not_found_for_group_id_not_found_error() -> None:
    admin = FakeGroupAdmin(descriptions={"ghost": kafka_exc(KafkaError.GROUP_ID_NOT_FOUND)})

    with pytest.raises(NotFound) as caught:
        describe_group(admin, "ghost")
    assert caught.value.code == "group_not_found"


def test_describe_timeout_is_a_504() -> None:
    admin = FakeGroupAdmin(descriptions={"g": TimeoutError()})

    with pytest.raises(KafkaTimeout):
        describe_group(admin, "g")


# --- resolve_targets ---------------------------------------------------------------------------


def test_resolve_targets_earliest_and_latest_cover_all_partitions() -> None:
    admin = FakeGroupAdmin(
        topics={"orders": 3},
        earliest={("orders", 0): 0, ("orders", 1): 5, ("orders", 2): 2},
        latest={("orders", 0): 10, ("orders", 1): 11, ("orders", 2): 12},
    )

    earliest = resolve_targets(admin, "orders", "earliest", None)
    latest = resolve_targets(admin, "orders", "latest", None)

    assert [(tp.partition, tp.offset) for tp in earliest] == [(0, 0), (1, 5), (2, 2)]
    assert [(tp.partition, tp.offset) for tp in latest] == [(0, 10), (1, 11), (2, 12)]
    assert {tp.topic for tp in earliest} == {"orders"}


def test_resolve_targets_timestamp_minus_one_falls_back_to_latest() -> None:
    admin = FakeGroupAdmin(
        topics={"orders": 2},
        by_time={("orders", 0): 3},  # partition 1 has nothing at/after the timestamp → -1
        latest={("orders", 0): 10, ("orders", 1): 8},
    )

    targets = resolve_targets(admin, "orders", "timestamp", 1_700_000_000_000)

    assert [(tp.partition, tp.offset) for tp in targets] == [(0, 3), (1, 8)]


def test_resolve_targets_timestamp_requires_a_non_negative_timestamp() -> None:
    admin = FakeGroupAdmin(topics={"orders": 1})

    for bad in (None, -5):
        with pytest.raises(ValidationFailed) as caught:
            resolve_targets(admin, "orders", "timestamp", bad)
        assert caught.value.field == "timestamp"


def test_resolve_targets_unknown_topic_is_not_found() -> None:
    with pytest.raises(NotFound) as caught:
        resolve_targets(FakeGroupAdmin(), "ghost", "latest", None)
    assert caught.value.code == "topic_not_found"


def test_resolve_targets_never_asks_the_brokers_about_a_topic_by_name() -> None:
    # A by-name metadata request can auto-create the topic on a broker that allows it.
    admin = FakeGroupAdmin(topics={"orders": 2})

    with pytest.raises(NotFound):
        resolve_targets(admin, "ghost", "latest", None)

    assert admin.list_topics_calls
    assert all(call["topic"] is None for call in admin.list_topics_calls)


def test_resolve_targets_unresolvable_offset_is_an_error_not_a_bogus_reset() -> None:
    admin = FakeGroupAdmin(topics={"orders": 1})  # no latest known → broker answers -1

    with pytest.raises(BrokerError):
        resolve_targets(admin, "orders", "latest", None)


# --- reset -------------------------------------------------------------------------------------


def empty_group_admin(**kwargs) -> FakeGroupAdmin:
    defaults = {
        "descriptions": {"g": group_description("g", "EMPTY")},
        "committed": {"g": committed_at(2)},
        "topics": {"orders": 2},
        "latest": {("orders", 0): 10, ("orders", 1): 6},
        "earliest": {("orders", 0): 0, ("orders", 1): 1},
    }
    return FakeGroupAdmin(**{**defaults, **kwargs})


def test_reset_to_latest_alters_and_returns_new_offsets_with_lag() -> None:
    admin = empty_group_admin()

    result = reset_offsets(admin, "g", "orders", "latest", None)

    assert admin.alter_calls == [("g", [("orders", 0, 10), ("orders", 1, 6)])]
    assert result == [
        OffsetView("orders", 0, committed=10, end=10, lag=0),
        OffsetView("orders", 1, committed=6, end=6, lag=0),
    ]


def test_reset_to_earliest_leaves_the_rest_as_lag() -> None:
    admin = empty_group_admin()

    result = reset_offsets(admin, "g", "orders", "earliest", None)

    assert admin.alter_calls == [("g", [("orders", 0, 0), ("orders", 1, 1)])]
    assert [v.lag for v in result] == [10, 5]


def test_reset_timestamp_minus_one_falls_back_to_latest() -> None:
    admin = empty_group_admin()

    reset_offsets(admin, "g", "orders", "timestamp", 5)

    assert admin.alter_calls == [("g", [("orders", 0, 10), ("orders", 1, 6)])]


@pytest.mark.parametrize("state", ["STABLE", "PREPARING_REBALANCING", "COMPLETING_REBALANCING"])
def test_reset_of_an_active_group_is_refused_before_anything_is_altered(state: str) -> None:
    admin = empty_group_admin(descriptions={"g": group_description("g", state)})

    with pytest.raises(Conflict) as caught:
        reset_offsets(admin, "g", "orders", "latest", None)

    assert caught.value.code == "group_not_empty"
    assert admin.alter_calls == []


def test_reset_of_an_unknown_group_is_not_found() -> None:
    admin = empty_group_admin(descriptions={})

    with pytest.raises(NotFound) as caught:
        reset_offsets(admin, "g", "orders", "latest", None)
    assert caught.value.code == "group_not_found"
    assert admin.alter_calls == []


@pytest.mark.parametrize(
    "code",
    [
        KafkaError.UNKNOWN_MEMBER_ID,
        KafkaError.NON_EMPTY_GROUP,
        KafkaError.ILLEGAL_GENERATION,
        KafkaError.REBALANCE_IN_PROGRESS,
        KafkaError.GROUP_SUBSCRIBED_TO_TOPIC,
    ],
)
def test_reset_broker_rejection_for_a_group_that_became_active_is_409(code: int) -> None:
    # The group was EMPTY when described but a consumer joined before the alter landed.
    admin = empty_group_admin()
    admin.alter_partition_error = KafkaError(code, "group is active")

    with pytest.raises(Conflict) as caught:
        reset_offsets(admin, "g", "orders", "latest", None)
    assert caught.value.code == "group_not_empty"


def test_reset_whole_call_rejection_is_409_too() -> None:
    admin = empty_group_admin()
    admin.alter_error = kafka_exc(KafkaError.UNKNOWN_MEMBER_ID)

    with pytest.raises(Conflict) as caught:
        reset_offsets(admin, "g", "orders", "latest", None)
    assert caught.value.code == "group_not_empty"


def test_reset_other_partition_errors_keep_their_mapping() -> None:
    admin = empty_group_admin()
    admin.alter_partition_error = KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED, "denied")

    with pytest.raises(Exception) as caught:
        reset_offsets(admin, "g", "orders", "latest", None)
    assert caught.value.code == "authorization_failed"  # type: ignore[attr-defined]


def test_reset_timestamp_without_a_timestamp_is_422_and_alters_nothing() -> None:
    admin = empty_group_admin()

    with pytest.raises(ValidationFailed):
        reset_offsets(admin, "g", "orders", "timestamp", None)
    assert admin.alter_calls == []


# --- create ------------------------------------------------------------------------------------


def test_create_commits_the_chosen_start_for_every_partition() -> None:
    admin = FakeGroupAdmin(
        topics={"orders": 2},
        earliest={("orders", 0): 0, ("orders", 1): 3},
        latest={("orders", 0): 10, ("orders", 1): 8},
    )

    create_group(admin, "fresh", "orders", "earliest")
    create_group(admin, "other", "orders", "latest")

    assert admin.alter_calls == [
        ("fresh", [("orders", 0, 0), ("orders", 1, 3)]),
        ("other", [("orders", 0, 10), ("orders", 1, 8)]),
    ]


@pytest.mark.parametrize("state", ["EMPTY", "STABLE", "PREPARING_REBALANCING"])
def test_create_on_an_existing_group_is_409(state: str) -> None:
    admin = FakeGroupAdmin(
        descriptions={"g": group_description("g", state)},
        topics={"orders": 1},
        latest={("orders", 0): 1},
    )

    with pytest.raises(Conflict) as caught:
        create_group(admin, "g", "orders", "latest")
    assert caught.value.code == "group_exists"
    assert admin.alter_calls == []


def test_create_with_committed_offsets_but_a_dead_state_is_409() -> None:
    admin = FakeGroupAdmin(
        committed={"g": committed_at(3, 1)}, topics={"orders": 1}, latest={("orders", 0): 9}
    )

    with pytest.raises(Conflict) as caught:
        create_group(admin, "g", "orders", "latest")
    assert caught.value.code == "group_exists"
    assert admin.alter_calls == []


def test_create_treats_group_id_not_found_as_absent() -> None:
    admin = FakeGroupAdmin(
        descriptions={"g": kafka_exc(KafkaError.GROUP_ID_NOT_FOUND)},
        topics={"orders": 1},
        latest={("orders", 0): 4},
    )

    create_group(admin, "g", "orders", "latest")

    assert admin.alter_calls == [("g", [("orders", 0, 4)])]


def test_create_on_an_unknown_topic_is_404_and_alters_nothing() -> None:
    admin = FakeGroupAdmin()

    with pytest.raises(NotFound) as caught:
        create_group(admin, "g", "ghost", "latest")
    assert caught.value.code == "topic_not_found"
    assert admin.alter_calls == []


def test_create_losing_a_race_to_a_joining_consumer_is_409_group_not_empty() -> None:
    admin = FakeGroupAdmin(topics={"orders": 1}, latest={("orders", 0): 4})
    admin.alter_partition_error = KafkaError(KafkaError.UNKNOWN_MEMBER_ID, "active")

    with pytest.raises(Conflict) as caught:
        create_group(admin, "g", "orders", "latest")
    assert caught.value.code == "group_not_empty"


# --- delete ------------------------------------------------------------------------------------


def test_delete_an_empty_group() -> None:
    admin = FakeGroupAdmin(descriptions={"g": group_description("g", "EMPTY")})

    delete_group(admin, "g")

    assert admin.delete_calls == [["g"]]


@pytest.mark.parametrize("state", ["STABLE", "PREPARING_REBALANCING", "COMPLETING_REBALANCING"])
def test_delete_of_an_active_group_is_409_and_deletes_nothing(state: str) -> None:
    admin = FakeGroupAdmin(descriptions={"g": group_description("g", state)})

    with pytest.raises(Conflict) as caught:
        delete_group(admin, "g")
    assert caught.value.code == "group_not_empty"
    assert admin.delete_calls == []


def test_delete_of_an_unknown_group_is_404() -> None:
    admin = FakeGroupAdmin()

    with pytest.raises(NotFound):
        delete_group(admin, "ghost")
    assert admin.delete_calls == []


def test_delete_broker_rejection_for_a_group_that_became_active_is_409() -> None:
    for code in (KafkaError.NON_EMPTY_GROUP, KafkaError.UNKNOWN_MEMBER_ID):
        admin = FakeGroupAdmin(descriptions={"g": group_description("g", "EMPTY")})
        admin.delete_error = kafka_exc(code)

        with pytest.raises(Conflict) as caught:
            delete_group(admin, "g")
        assert caught.value.code == "group_not_empty"


def test_every_admin_call_carries_the_request_timeout() -> None:
    admin = empty_group_admin()

    reset_offsets(admin, "g", "orders", "latest", None)
    delete_group(admin, "g")

    assert admin.kwargs_seen
    assert all(kw == {"request_timeout": 10.0} for kw in admin.kwargs_seen)


# --- coordinator flux --------------------------------------------------------------------------


@pytest.fixture
def fast_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("kafka_web.services.groups.RETRY_DELAY_S", 0.0)


@pytest.mark.parametrize(
    "code",
    [
        KafkaError.NOT_COORDINATOR,
        KafkaError.COORDINATOR_NOT_AVAILABLE,
        KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
    ],
)
def test_describe_retries_while_the_group_coordinator_is_moving(
    code: int, fast_retries: None
) -> None:
    admin = empty_group_admin()
    admin.describe_failures = [kafka_exc(code), kafka_exc(code)]

    assert describe_group(admin, "g").state == "empty"
    assert admin.describe_calls == 3


def test_committed_offsets_and_alter_retry_too(fast_retries: None) -> None:
    admin = empty_group_admin()
    admin.list_offset_failures = [kafka_exc(KafkaError.NOT_COORDINATOR)]
    admin.alter_failures = [kafka_exc(KafkaError.COORDINATOR_NOT_AVAILABLE)]

    assert describe_group(admin, "g").offsets[0].committed == 2
    reset_offsets(admin, "g", "orders", "latest", None)

    assert len(admin.alter_calls) == 2  # the failed attempt and the one that landed


def test_a_coordinator_that_never_settles_gives_up_within_the_timeout() -> None:
    admin = empty_group_admin()
    admin.describe_failures = [kafka_exc(KafkaError.NOT_COORDINATOR)] * 1000

    with pytest.raises(BrokerError):
        describe_group(admin, "g", timeout=0.3)
    assert 1 < admin.describe_calls < 1000


def test_other_failures_are_not_retried(fast_retries: None) -> None:
    admin = empty_group_admin()
    admin.describe_failures = [kafka_exc(KafkaError.GROUP_AUTHORIZATION_FAILED)]

    with pytest.raises(Forbidden):
        describe_group(admin, "g")
    assert admin.describe_calls == 1
