import os
import json
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
from slack_sdk import WebClient

FLOWER_USERNAME = os.getenv("FLOWER_USERNAM")
FLOWER_PASSWORD = os.getenv("FLOWER_PASSWORD")
FLOWER_SERVER_PORT = os.getenv("FLOWER_SERVER_PORT")
FLOWER_LOCAL_IP = "0.0.0.0"
FLOWER_IP= os.getenv("FLOWER_SERVER_IP")
SALCK_BOT_ID = os.getenv("SALCK_BOT_ID")
TOKEN = os.getenv("SALCK_TOKEN")
CHANNEL_ID = os.getenv("SALCK_CHANNEL_ID")

FLOWER_SERVER_ADDRESS  =f"http://{FLOWER_IP}:{FLOWER_SERVER_PORT}/api/workers"
AUTH = HTTPBasicAuth(FLOWER_USERNAME, FLOWER_PASSWORD)


class SLACK_MSG:
    SLACK_SERVER_NOT_FOUND="server not found"
    SLACK_SERVER_AUTH_ERROR_MSG = "auth error"
    SLACK_SERVER_SERVER_NAME = "*Flower server:[SERVER1]*"
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

    def get_msg(self):
        result = self.client.conversations_history(
            channel=self.channel_id, 
            limit= 2
        )
        return result.data['messages']

    def cheack_worker_status(self, response):
        off_line_woker = []
        for i,v in response.json().items():
            if not bool(v):
                off_line_woker.append(i)
        if off_line_woker:
            data = SLACK_MSG.SLACK_OFF_LINE_WORKER_MSG
            worker_list = [{"value":f"`{i}`"} for i in off_line_woker]
            data[0]["fields"] = worker_list
            return self.send_msg(SLACK_MSG.SLACK_SERVER_SERVER_NAME, data)

        else:
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
        response = requests.get(self.flower_address + "?status=1", auth=self.auth)
        if response.status_code == 500:
            #self.send_msg(SLACK_MSG.SLACK_SERVER_NOT_FOUND)
            raise Exception(SLACK_MSG.SLACK_SERVER_NOT_FOUND)
        if response.status_code == 401:
            #self.send_msg(SLACK_MSG.SLACK_SERVER_AUTH_ERROR_MSG)
            raise Exception(SLACK_MSG.SLACK_SERVER_NOT_FOUND)
        if not self.cheack_overlap_status(response):
            return None
        return self.cheack_worker_status(response)
        
        
if __name__ == '__main__':
    run = PassSlacktoWorkerInfo(TOKEN, CHANNEL_ID, FLOWER_SERVER_ADDRESS, AUTH)
    run.worker_status_check()