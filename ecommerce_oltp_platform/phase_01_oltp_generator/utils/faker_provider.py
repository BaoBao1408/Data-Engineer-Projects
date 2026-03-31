from faker import Faker
import random

faker = Faker("en_US")

def seed_all(seed: int = 42):
    Faker.seed(seed)
    random.seed(seed)
