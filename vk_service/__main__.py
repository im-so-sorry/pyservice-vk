import vk_api
from vk_api.utils import get_random_id

import environs

from vk_service.worker import MessageSenderWorker

env = environs.Env()
env.read_env()


def main():
    # vk_session = vk_api.VkApi(token=env("VK_KEY"))
    # vk = vk_session.get_api()
    #
    # attachments = ["video85635407_165186811_69dff3de4372ae9b6e"]
    #
    # vk.messages.send(
    #     # domain="arck1",
    #     user_ids="98449858",
    #     attachment=','.join(attachments),
    #     random_id=get_random_id(),
    #     message="test",
    # )

    worker = MessageSenderWorker()

    worker.listen()


if __name__ == "__main__":
    main()
