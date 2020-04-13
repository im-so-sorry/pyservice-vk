from time import sleep
from typing import Dict, List, Tuple, Any

from kafka.consumer.fetcher import ConsumerRecord
from vk_api.utils import get_random_id

from vk_service.kafka_client import KafkaClient
import vk_api
from vk_service import settings

MESSAGE_TEMPLATE = """
Новое событие
Теги = {tags}
Тип = {event_type}
Дата создания = {creation_time}
Ссылка = {event_url}

{text}
"""


class MessageSenderWorker(object):
    def __init__(self):
        self.client = KafkaClient(
            topics=settings.KAFKA_TOPICS,
            bootstrap_servers=settings.KAFKA_HOSTS,
            group_id=settings.KAFKA_GROUP_ID,
        )
        self.vk_session = vk_api.VkApi(token=settings.VK_TOKEN)
        self.vk = self.vk_session.get_api()

    def send_message(self, user_ids: str = None, attachments: List[str] = None,
                     message: str = None):
        self.vk.messages.send(
            user_ids=user_ids,
            message=message,
            attachment=attachments,
            random_id=get_random_id(),
        )
        sleep(20)

    def process_message(self, msg: ConsumerRecord) -> Tuple[Any, bool]:
        data = msg.value

        users_ids = data.get('user_ids', [98449858])
        event = data.get('event', {})

        attachments: List[Dict] = event.get("attachments", [])
        attachments_paths = []

        for attachment_dict in attachments:
            attachment_type = attachment_dict.get("type")
            attachment = attachment_dict.get(attachment_type)

            attachment_owner_id = attachment.get("owner_id")
            media_id = attachment.get("id")

            if not all((attachment_type, attachment, attachment_owner_id, media_id)):
                continue

            path = "{type}{owner_id}_{media_id}".format(
                type=attachment_type,
                owner_id=attachment_owner_id,
                media_id=media_id
            )
            attachments_paths.append(path)

        message = MESSAGE_TEMPLATE.format(
            tags=event.get('tags', []),
            event_type=event.get('event_type', "undefined"),
            creation_time=event.get('creation_time', ""),
            event_url=event.get('event_url', ""),
            text=event.get('text', "")
        )

        self.send_message(user_ids=users_ids, attachments=attachments_paths, message=message)
        return 0, True

    def listen(self):
        self.client.listen(self.process_message)
