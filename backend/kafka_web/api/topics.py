"""Topic endpoints (spec §2)."""

from fastapi import APIRouter

from kafka_web.api.deps import RegistryDep
from kafka_web.api.schemas import TopicConfigView, TopicListView
from kafka_web.services import topics as topic_service

router = APIRouter()


@router.get("/clusters/{name}/topics")
def list_topics(name: str, registry: RegistryDep) -> TopicListView:
    admin = registry.get(name).admin
    return TopicListView.model_validate({"topics": topic_service.list_topics(admin)})


@router.get("/clusters/{name}/topics/{topic}/config")
def topic_config(name: str, topic: str, registry: RegistryDep) -> TopicConfigView:
    admin = registry.get(name).admin
    return TopicConfigView.model_validate(topic_service.describe_topic(admin, topic))
