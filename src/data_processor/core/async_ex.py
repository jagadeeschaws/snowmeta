import asyncio
import os
import random
import time
import uuid

import aiohttp
from aiohttp_retry import RetryClient, ExponentialRetry
from dotenv import load_dotenv
from faker import Faker

fake = Faker()
load_dotenv('.env')


class MyLogger():
    def debug(self, *args, **kwargs):
        print('[debug]:', *args, **kwargs)


async def mparticle_connector(session):
    """
    This function loads the user UID token as a partner ID and a custom event
    :param session: Retry Session
    :return: status: API Status
    """

    request_body = {
        "events": [
            {
                "data": {
                    "event_name": "Loyalty",
                    "custom_event_type": "other",
                    "source_message_id": str(str(uuid.uuid4())),
                    "custom_attributes": {
                        "TRANS_ID": str(uuid.uuid4()),
                        "ORDER_ID": str(uuid.uuid4()),
                        "EMPLOYEE_ID": str(uuid.uuid4()),
                        "PROFILE_ID": str(uuid.uuid4()),
                        "REST_ID": str(uuid.uuid4()),
                        "BRAND_ID": random.choice(['AR567', 'BR321', 'BW887', 'DK890', 'JJ555', 'SO124']),
                        "BUSINESS_DATE": str(fake.date_time_this_month()),
                        "EMPLOYEE_NAME": fake.name(),
                        "CHECK_NBR": str(uuid.uuid4()),
                        "MEMBER_CARD_NBR": str(uuid.uuid4()),
                        "DOLLAR_NET_VALUE": random.uniform(5.99, 99.99),
                        "ELIGIBLE_REVENUE": random.uniform(5.99, 999.99),
                        "METHOD_OF_ATTACHMENT": random.choice(['Manual', 'Automatic']),
                        "TRANS_STATUS": random.choice(['Pending', 'Complete', 'Cancelled']),
                        "REASON_CODE": random.randint(500, 505),
                        "MIN_DAY_PART_ID": str(uuid.uuid4()),
                        "MAX_DAY_PART_ID": str(uuid.uuid4()),
                        "SUSPEND_TRANS_ID": str(uuid.uuid4()),
                        "SUSPEND_TRANS_STATUS": random.choice(['Pending', 'Complete', 'Cancelled']),
                        "SOURCE_SYSTEM_NM": random.choice(['POS', 'App', 'Web']),
                        "LOAD_TYPE": random.choice(['Full', 'Partial']),
                        "LOAD_ID": str(uuid.uuid4()),
                        "LOAD_DTTM": str(fake.date_time_this_month()),
                        "UPDATE_ID": str(uuid.uuid4()),
                        "UPDATE_DTTM": str(fake.date_time_this_month()),
                        "FILE_NAME": f"{fake.word()}_{time.time()}"
                    }
                },
                "event_type": "custom_event"
            }
        ],
        "user_identities": {
            # EMAIL_ID
            # Pre-pending an random word to prevent faker from running out of unique emails
            "email": f"{fake.word()}{fake.email()}",
            # MOBILE_NBR
            # FORMAT INFO: https://en.wikipedia.org/wiki/MSISDN
            "mobile_number": fake.msisdn(),
            # MEMBER_ID
            "customer_id": str(uuid.uuid4()),
            # LOYALTY_PROGRAM_ID
            "other": str(uuid.uuid4()),
            # CLOSEST_STORE_ID
            "other_id_2": str(uuid.uuid4()),
            # HOUSEHOLD_ID
            "other_id_3": str(uuid.uuid4()),
            # BRAND_ID
            "other_id_4": str(uuid.uuid4()),
            # MEMBER_INSPIRE_ID
            "other_id_5": str(uuid.uuid4()),
            # EXPERIAN_ID
            "other_id_6": str(uuid.uuid4()),
            # MDM_ID
            "other_id_7": str(uuid.uuid4()),
            # LOYALTY_CARD_NBR
            "other_id_8": str(uuid.uuid4()),
        }, "user_attributes":
            {
                "$firstname": fake.first_name(),
                "$lastname": fake.last_name(),
                "MIDDLE_INITIAL_TXT": fake.last_name(),
                "DOB_DT": str(fake.date_this_century()),
                "BIRTH_MONTH_NBR": fake.month(),
                "BIRTH_YEAR_NBR": fake.year(),
                "IS_BIRTH_DATE_IMPLIED_IND": random.choice([True, False]),
                "GENDER_TYP": random.choice(['M', 'F', 'U']),
                "ENROLLMENT_CHANNEL_TYP": random.choice(['POS', 'App', 'Web']),
                "MEMBER_STATUS_CD": random.choice([True, False]),
                "POINT_BALANCE_NBR": random.randint(999, 99999),
                "MEMBERSHIP_STATUS_CD": random.choice([True, False]),
                "PROFILE_COMPLETED_STATUS_IND": random.choice([True, False]),
                "DELIVER_ABILITY_STATUS_IND": random.choice([True, False]),
                "ADDRESS_LINE_1_TXT": fake.street_address(),
                "ADDRESS_LINE_2_TXT": fake.street_name(),
                "CITY_NM": fake.city(),
                "STATE_CD": fake.state(),
                "ZIP_CD": fake.zipcode(),
                "COUNTRY_CD": fake.current_country_code(),
                "EMAIL_OPT_OUT_IND": random.choice([True, False]),
                "PUSH_NOTIFICATION_OPT_IN_IND": random.choice([True, False]),
                "SMS_OPT_IN_IND": random.choice([True, False]),
                "PRIVACY_IND": random.choice([True, False]),
                "UNSUBSCRIBE_DT": str(fake.date_time_this_month()),
                "POINTS_EXPIRE_DT": str(fake.future_datetime()),
                "ENROLL_START_DT": str(fake.date_time_this_month()),
                "LAST_LOGIN_DT": str(fake.date_time_this_month()),
                "LAST_STATUS_CHANGE_DT": str(fake.date_time_this_month()),
                "PROFILE_COMPLETION_DT": str(fake.date_time_this_month()),
                "SOURCE_SYSTEM_NM": random.choice(['UDP', 'External', 'Internal']),
                "CDM_LOAD_DT": str(fake.date_time_this_month()),
                "LOAD_ID": str(uuid.uuid4()),
                "LOAD_DTTM": str(fake.date_time_this_month()),
                "SUBSCRIBER_KEY": str(uuid.uuid4()),
                "SUBSCRIBER_SOURCE_NM": random.choice(['UDP', 'External', 'Internal']),
                "EXPERIAN_STATUS_TXT": random.choice([True, False]),
                "INSPIRE_CUSTOMER_TYP": random.choice(['Premium', 'Premium+']),
                "MDM_ID_DELETED_IND": random.choice([True, False]),
            },

        "environment": f"{os.getenv('FEED_ENV')}"
    }

    retry_options = ExponentialRetry(attempts=4, statuses=[429])
    async with session.post(f"{os.getenv('FEED_URL')}", json=request_body,
                            auth=aiohttp.BasicAuth(os.getenv('FEED_KEY'), os.getenv('FEED_SECRET')),
                            retry_options=retry_options) as resp:
        return resp.status


async def main():
    async with RetryClient(logger=MyLogger()) as retry_client:
        task = []
        for _ in range(1000):
            task.append(mparticle_connector(session=retry_client))
        result = await asyncio.gather(*task, return_exceptions=True)
        for res in result:
            print(res)


t1 = time.perf_counter()
asyncio.run(main())
print("Loaded 1000 events")
print("--- %s seconds ---" % (time.perf_counter() - t1))