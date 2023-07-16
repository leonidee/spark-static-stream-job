from __future__ import annotations

import json
import logging
import os
import re

import requests
from dotenv import find_dotenv, load_dotenv

logger = logging.getLogger(name=__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format=r"[%(asctime)s] {%(name)s.%(funcName)s:%(lineno)d} %(levelname)s: %(message)s",
    datefmt=r"%Y-%m-%d %H:%M:%S",
)


def get_iam_token() -> bool:
    logger.debug("Loading environment")
    find_dotenv()
    load_dotenv()

    YC_OAUTH_TOKEN = os.getenv("YC_OAUTH_TOKEN")
    logger.debug("Done")

    if not YC_OAUTH_TOKEN:
        raise EnvironmentError("'YC_OAUTH_TOKEN' not set")

    logger.debug("Sending request to get iam token")
    response = requests.post(
        url="https://iam.api.cloud.yandex.net/iam/v1/tokens",
        json={"yandexPassportOauthToken": YC_OAUTH_TOKEN},
    )
    response.raise_for_status()

    logger.debug("Success")
    response = response.json()

    token_key = next(
        _ for _ in response.keys() if re.search("iamtoken", _, re.IGNORECASE)
    )

    logger.debug("Setting as environment variable")
    os.environ["YC_IAM_TOKEN"] = response[token_key]

    if not os.getenv("YC_IAM_TOKEN"):
        raise EnvironmentError("Something went wrong! 'YC_IAM_TOKEN' not set")

    logger.debug("All success")

    return True


def get_list_of_zone_ids() -> ...:
    """
     {
        "zones": [
            {"id": "ru-central1-a", "status": "UP"},
            {"id": "ru-central1-b", "status": "UP"},
            {"id": "ru-central1-c", "status": "UP"},
        ]
    }
    """

    find_dotenv()
    load_dotenv()
    YC_IAM_TOKEN = os.getenv("YC_IAM_TOKEN")

    URL = "https://compute.api.cloud.yandex.net/compute/v1/zones"

    response = requests.get(
        url=URL,
        headers={"Authorization": f"Bearer {YC_IAM_TOKEN}"},
    )

    response.raise_for_status()

    response = response.json()
    print(response)


def get_list_of_disk_types() -> ...:
    """
    {
        "diskTypes": [
            {"id": "network-hdd", "description": "Network storage with HDD backend"},
            {"id": "network-ssd", "description": "Network storage with SSD backend"},
            {
                "id": "network-ssd-nonreplicated",
                "description": "Non-replicated network storage with SSD backend",
            },
        ]
    }
    """
    find_dotenv()
    load_dotenv()
    YC_IAM_TOKEN = os.getenv("YC_IAM_TOKEN")

    URL = "https://compute.api.cloud.yandex.net/compute/v1/diskTypes"

    response = requests.get(
        url=URL,
        headers={"Authorization": f"Bearer {YC_IAM_TOKEN}"},
    )
    response.raise_for_status()

    response = response.json()
    print(response)


def get_list_of_clouds() -> ...:
    """
    {
        "clouds": [
            {
                "id": "b1gcj63q69dgi7jup4i5",
                "createdAt": "2022-10-06T17:34:33Z",
                "name": "cloud-leonidgrishenkov",
                "organizationId": "bpfeomvq11rln5op3lae",
            }
        ]
    }
    """
    find_dotenv()
    load_dotenv()
    YC_IAM_TOKEN = os.getenv("YC_IAM_TOKEN")

    URL = "https://resource-manager.api.cloud.yandex.net/resource-manager/v1/clouds"

    response = requests.get(
        url=URL,
        headers={"Authorization": f"Bearer {YC_IAM_TOKEN}"},
    )
    response.raise_for_status()

    response = response.json()
    print(response)


def get_list_of_folders() -> ...:
    """
    {
        "folders": [
            {
                "id": "b1g6g4do1qltb9n60447",
                "cloudId": "b1gcj63q69dgi7jup4i5",
                "createdAt": "2022-10-06T17:34:33Z",
                "name": "default",
                "status": "ACTIVE",
            }
        ]
    }
    """
    find_dotenv()
    load_dotenv()
    YC_IAM_TOKEN = os.getenv("YC_IAM_TOKEN")

    URL = "https://resource-manager.api.cloud.yandex.net/resource-manager/v1/folders"
    PARAMS = dict(cloudId="b1gcj63q69dgi7jup4i5")

    response = requests.get(
        url=URL,
        params=PARAMS,
        headers={"Authorization": f"Bearer {YC_IAM_TOKEN}"},
    )
    response.raise_for_status()

    response = response.json()
    print(response)


def get_list_of_service_accounts() -> ...:
    """
    {
        "serviceAccounts": [
            {
                "id": "ajedh73oau2t4qtvpuag",
                "folderId": "b1g6g4do1qltb9n60447",
                "createdAt": "2022-10-18T11:36:01Z",
                "name": "leonide",
            }
        ]
    }
    """
    find_dotenv()
    load_dotenv()
    YC_IAM_TOKEN = os.getenv("YC_IAM_TOKEN")

    URL = "https://iam.api.cloud.yandex.net/iam/v1/serviceAccounts"
    PARAMS = dict(folderId="b1g6g4do1qltb9n60447")

    response = requests.get(
        url=URL,
        params=PARAMS,
        headers={"Authorization": f"Bearer {YC_IAM_TOKEN}"},
    )
    response.raise_for_status()

    response = response.json()
    print(response)


def create_host() -> ...:
    """ """
    from http import HTTPStatus
    from pathlib import Path

    find_dotenv()
    load_dotenv()

    YC_IAM_TOKEN = os.getenv("YC_IAM_TOKEN")
    DISK_SIZE = str(50 * 1024 * 1024 * 1024)
    RAM = str(16 * 1024 * 1024 * 1024)
    CPU_COUNT = 8

    # PUB_KEY_PATH = "/Users/leonidgrisenkov/.ssh/id_rsa.pub"  # todo replace with getenv
    # PUB_KEY = Path(PUB_KEY_PATH).read_text(encoding="UTF-8")
    logger.info("Creating host")

    msg = {
        "folderId": "b1g6g4do1qltb9n60447",
        "name": "de-debian-16",
        "zoneId": "ru-central1-b",
        "platformId": "standard-v3",
        "resourcesSpec": {"memory": f"{RAM}", "cores": f"{CPU_COUNT}"},
        "metadata": {
            "user-data": f"#cloud-config\nusers:\n  - name: yc-user\n    groups: sudo\n    shell: /bin/bash\n    sudo: ['ALL=(ALL) NOPASSWD:ALL']\n     ssh-authorized-keys:\n      - ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQDMMPrWVK4wsS7Z3s7cqh/11kryCS+i/jR0EIB1RXEmuThO0lACv2yS6zMPtNFGHGepPsvsC8Zjszu0Ntoe6kW/VOLtc7et2NBXVyfW/ZO0ttpySDMjuhLy+2uchv98vDvtnZ5fI1/03PSkmnuH+NVImLJK3E4Tc30G4JkfNkW5u7px8AjANtoxYJ5/wMCh5M7pgQm3Wro79gdTwTuZlSt9x629UoURnLEtY3mr6wQRfLySDBjbz7hqPrVUhzU0iaZWQubY9cGGx9SWg6Oaje5EzBFj/5Xs4/Ue2bIlEQv0QIgc2RiQR0mTTweCVc11NeER66z9faQBCSMo3atUSgnLqovTyxx+Zezkdf4qPc5IoY73AAO6rQUgmCN8/9fWlc8ZtPKqR5Sdsi7yMoortrO5u2QkLJKZTVBWuH5ef9f2Lc9Zc0BaveipLuhhjxH9ItvDZO5mmyZUFXMgrEnSntW79eDBaDoePb1mawrDWFjrDZJhSnuTlUHxOrpn1aweeGk7dHe+OXLH01m3Xz0i7xboVnp/nnFywecHvfd+Gh5IEAhebOmynwxx4NFqm7pzQoSLB+nliSqe2KJehyQtj56BqxsMnKMXbOrR9QCSukNaHCr33gHGFADYgKLD1yeKEZRoXjoaIAE7RcTLiJE1dxWt2esfvlLbxH0JAiuzhSOKQQ==leonide@MacBook-Pro-Leonide.local",
        },
        "bootDiskSpec": {
            "diskSpec": {"size": f"{DISK_SIZE}", "imageId": "fd843htdp8usqsiji0bb"}
        },
        "networkInterfaceSpecs": [
            {
                "subnetId": "e2lg9dqv372aab17gdn1",
                "primaryV4AddressSpec": {"oneToOneNatSpec": {"ipVersion": "IPV4"}},
                "securityGroupIds": ["enp9qq7b5fn7f20erdcg"],
            }
        ],
        "serviceAccountId": "ajedh73oau2t4qtvpuag",
    }

    logger.debug(f"{msg=}")

    response = requests.post(
        url="https://compute.api.cloud.yandex.net/compute/v1/instances",
        data=json.dumps(msg),
        headers={
            "Authorization": f"Bearer {YC_IAM_TOKEN}",
            "Content-Type": "application/json",
        },
    )

    if response.status_code == HTTPStatus(value=400):
        response = response.json()
        logger.warning(response.get("message"))

    elif response.status_code != HTTPStatus(value=200):
        response = response.json()
        logger.warning(response)

        response.raise_for_status()

    else:
        logger.debug("Success. Request sent")


def main() -> ...:
    create_host()


if __name__ == "__main__":
    main()
