import os
import json
import docker
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth
from slack_sdk import WebClient

load_dotenv(os.path.join(os.path.abspath(os.path.dirname(__file__)), '.env'))

FLOWER_USERNAME = os.getenv("FLOWER_USERNAME")
FLOWER_PASSWORD = os.getenv("FLOWER_PASSWORD")
FLOWER_SERVER_PORT = os.getenv("FLOWER_SERVER_PORT")
FLOWER_DOCKER_NAME = os.getenv("FLOWER_DOCKER_NAME")
FLOWER_LOCAL_IP = "0.0.0.0"
FLOWER_IP= os.getenv("FLOWER_SERVER_IP")
SALCK_BOT_ID = os.getenv("SALCK_BOT_ID")
TOKEN = os.getenv("SALCK_TOKEN")
CHANNEL_ID = os.getenv("SALCK_CHANNEL_ID")
ERROR_MSG_CHANNEL_ID = os.getenv("ERROR_MSG_CHANNEL_ID")


FLOWER_SERVER_ADDRESS  =f"http://{FLOWER_IP}:{FLOWER_SERVER_PORT}/api/workers"
AUTH = HTTPBasicAuth(FLOWER_USERNAME, FLOWER_PASSWORD)


def get_docker(docker_name: str) -> docker:
    client = docker.from_env()
    container = client.containers.list(all=True, filters={'name': docker_name})
    if container:
        return client.containers.get(container[0].id)
    else:
        return False

def restart_docker(container: docker) -> bool:
    try:
        container.stop()
        container.start()
    except Exception as e:
        print(e)
        return False
    return True


class SLACK_MSG:
    SLACK_SENTRY_ERROR_MSG = "(2013, 'Lost connection to MySQL server during query')"
    SLACK_SERVER_NOT_FOUND="server not found"
    SLACK_SERVER_ERROR = "server error"
    SLACK_SERVER_AUTH_ERROR_MSG = "auth error"
    SLACK_SERVER_SERVER_NAME = "*Flower server:[SERVER1]*"
    SLACK_SERVER_NOT_FOUND_MSG = [
            {
                "color": "#FF2C2C",
                "title": ":no_entry_sign: [ERROR] Flower Server Not Found",
                "title_link": f"http://{FLOWER_IP}:{FLOWER_SERVER_PORT}/",
                "text": f":sob: *Flower server not found*\nflower url : <http://{FLOWER_IP}:{FLOWER_SERVER_PORT}/|url>",
                "fields": None,
                "footer": "Lionrocket flower api",
                "footer_icon": "https://lionrocket.ai/lion_og.jpg"
        }
    ]
    SLACK_SERVER_ERROR_MSG = [
            {
                "color": "#FF2C2C",
                "title": ":no_entry_sign: [ERROR] Flower Request Server Error",
                "title_link": f"http://{FLOWER_IP}:{FLOWER_SERVER_PORT}/",
                "text": f":sob: *Flower Reqeust server Error*\nflower url : <http://{FLOWER_IP}:{FLOWER_SERVER_PORT}/|url>",
                "fields": None,
                "footer": "Lionrocket flower api",
                "footer_icon": "https://lionrocket.ai/lion_og.jpg"
        }
    ]
    SLACK_OFF_LINE_WORKER_MSG = [
            {
                "color": "#FF2C2C",
                "title": ":no_entry_sign: [ERROR] Celery Worker Sutdown & Off-line",
                "title_link": f"http://{FLOWER_IP}:{FLOWER_SERVER_PORT}/",
                "text": f"*WORKER LIST*\nflower url : <http://{FLOWER_IP}:{FLOWER_SERVER_PORT}/|url>",
                "fields": None,
                "footer": "Lionrocket flower api",
                "footer_icon": "https://lionrocket.ai/lion_og.jpg"
        }
    ]


class PassSlacktoWorkerInfo:
    def __init__(self,token, channel_id, flower_address, auth):
        self.client = WebClient(token=token)
        self.channel_id =channel_id
        self.flower_address = flower_address
        self.auth = auth
 
    def send_msg(self, slack_msg: dict=None , atta: list=None):
        if atta is not None:
            return self.client.chat_postMessage(
            channel=self.channel_id,
            text = slack_msg,
            attachments=json.dumps(atta)
        )
        else:
            return self.client.chat_postMessage(
                channel=self.channel_id,
                text = slack_msg,
            )

    def get_msg(self, channel=None):
        if channel is None:
            channel = self.channel_id
        result = self.client.conversations_history(
            channel = channel, 
            limit= 10,
        )
        return result.data['messages']

    def get_error_msg(self,):
        msg = self.get_msg(channel=ERROR_MSG_CHANNEL_ID)
        for i in msg:
            dt = datetime.fromtimestamp(float(i["ts"])) + timedelta(hours=24)
            now_time = datetime.now()
            if "bot_id" in i:
                if dt <=  now_time and SLACK_MSG.SLACK_SENTRY_ERROR_MSG in i["attachments"][0]['text']:
                    return True
        return False

    def restart_flower_server(self):
        docker_container = get_docker(FLOWER_DOCKER_NAME)
        if not docker_container:
            raise Exception("run docker name not found")
        reuslt = restart_docker(docker_container)
        print(reuslt)
        return None

    def cheack_error_worker_status(self):
        reuslt_error_msg = self.get_error_msg()
        if reuslt_error_msg:
            self.restart_flower_server()
            return True
        return False

    def cheack_worker_status(self, response):
        off_line_woker = []
        for i,v in response.json().items():
            if not bool(v):
                off_line_woker.append(i)
        if off_line_woker:
            result = self.cheack_error_worker_status()
            if not result:
                data = SLACK_MSG.SLACK_OFF_LINE_WORKER_MSG
                worker_list = [{"value":f"`{i}`"} for i in off_line_woker]
                data[0]["fields"] = worker_list
                return self.send_msg(SLACK_MSG.SLACK_SERVER_SERVER_NAME, data)
        return None

    def cheack_overlap_status(self, response: list):
        msg = self.get_msg()
        for i in msg:
            dt = datetime.fromtimestamp(float(i["ts"])) + timedelta(hours=24)
            now_time = datetime.now()
            if "bot_id" in i and i["bot_id"] == SALCK_BOT_ID:
                if "fields" in i["attachments"][0]:
                    fields_worker_list  = [v["value"] for v in i["attachments"][0]["fields"]]
                    response_worker_list= [ i for i,v in response.json().items() if bool(v)]
                    if response_worker_list.sort() == fields_worker_list.sort():
                        if dt <=  now_time:
                            break
                        return False
        return True

    def worker_status_check(self):
        try:
            response = requests.get(self.flower_address + "?status=1", auth=self.auth)
        except:
            self.send_msg(SLACK_MSG.SLACK_SERVER_NOT_FOUND_MSG)
            raise Exception(SLACK_MSG.SLACK_SERVER_NOT_FOUND)
        if response.status_code == 500:
            self.send_msg(SLACK_MSG.SLACK_SERVER_ERROR_MSG)
            raise Exception(SLACK_MSG.SLACK_SERVER_ERROR)
        if response.status_code == 401:
            #self.send_msg(SLACK_MSG.SLACK_SERVER_AUTH_ERROR_MSG)
            raise Exception(SLACK_MSG.SLACK_SERVER_NOT_FOUND)
        if not self.cheack_overlap_status(response):
            return None
        return self.cheack_worker_status(response)
        
        
if __name__ == '__main__':
    run = PassSlacktoWorkerInfo(TOKEN, CHANNEL_ID, FLOWER_SERVER_ADDRESS, AUTH)
    run.worker_status_check()
