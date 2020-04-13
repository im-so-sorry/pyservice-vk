from environs import Env

env = Env()
env.read_env()

KAFKA_HOSTS = env.list("KAFKA_HOSTS", ("localhost:9093",))
KAFKA_GROUP_ID = env("KAFKA_GROUP_ID", "vk_service")
KAFKA_TOPICS = env.list("KAFKA_TOPICS", ("vk_service",))


VK_TOKEN = env("VK_TOKEN")
