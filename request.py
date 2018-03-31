from urllib import request
import requests
import json
from time import sleep

url = 'https://api.telegram.org/bot495650995:AAFo50WpmMRMwQqhdpadQ9NgYlF3KiGmBF8/'

def get_updates():
    # req = 'https://api.telegram.org/bot495650995:AAFo50WpmMRMwQqhdpadQ9NgYlF3KiGmBF8/getUpdates'

    data = None

    try:
        resp = requests.get(url + 'getUpdates')
        data = resp.json()
        # for item in data['result']:
        #     for k, v in item.items():
        #         print(k, ": ", v)
    except Exception as e:
        print("Error: ", e)

    return data

def last_update(data):
    results = data['result']
    last = len(results) - 1
    return results[last]

def get_chat_id(update):
    chat_id = update['message']['chat']['id']
    return chat_id

def send_response(chat_id, text):
    params = {
        'chat_id': chat_id,
        'text': text,
    }
    # req = 'https://api.telegram.org/bot495650995:AAFo50WpmMRMwQqhdpadQ9NgYlF3KiGmBF8/sendMessage?chat_id={}&text={}'.\
    #     format(chat_id, text)
    data = None
    try:
        resp = requests.post(url + 'sendMessage', data=params)
        data = resp.json()
    except Exception as e:
        print("Error: ", e)
    return data

def main():
    update_id = last_update(get_updates())['update_id']
    while True:
        if update_id == last_update(get_updates())['update_id']:
            send_response(get_chat_id(last_update(get_updates())), "My text")
            update_id +=1
        sleep(1)

if __name__ == '__main__':
    main()




