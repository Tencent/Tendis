import json
import requests
import sys
import time

def getRenderTaskId(renderUrl, bk_app_code, bk_app_secret, bk_username, bk_biz_id, dashboard_uid, panel_id, app, cluster_domain, start, end):
    data = {
        "type": "dashboard",
        "options": {
            "bk_biz_id": bk_biz_id,
            "dashboard_uid": dashboard_uid,
            "panel_id": panel_id,
            "height": 300,
            "width": 1080,
            "variables": {"app": [app], "cluster_domain": [cluster_domain]},
            "start_time": start,
            "end_time": end
        }
    }

    response = requests.post(
        renderUrl,
        headers={
            "X-Bkapi-Authorization": json.dumps({"bk_app_code": bk_app_code, "bk_app_secret": bk_app_secret, "bk_username": bk_username})
        },
        json=data
    )

    if response.status_code == 200:
        response_data = response.json()
        print(response_data)
        request_id = response.headers.get('X-Bkapi-Request-Id')
        print(f"X-bkapi-Request-Id: {request_id}")
        task_id = response_data["data"]["task_id"]
        print(f"task_id: {task_id}")
        return task_id
    else:
        print(f"error code: {response.status_code}")
        sys.exit(1)
def getrenderCurve(pngUrl, bk_app_code, bk_app_secret, bk_username, task_id):
    params = {
        "task_id": task_id
    }
    headers = {
        "X-Bkapi-Authorization": json.dumps({
            "bk_app_code": bk_app_code,
            "bk_app_secret": bk_app_secret,
            "bk_username": bk_username
        })
    }
    print(f"here task_id is {task_id}")
    response = requests.get(pngUrl, headers=headers, params=params)
    image_url=""
    response_data = response.json()
    try:
        image_url = response_data["data"]["image_url"]
    except KeyError:
        print("without image_url")
        sys.exit(1)
    print(image_url)
    return image_url

def download_image(image_url, filename):
    try:
        image_response = requests.get(image_url, stream=True)
        image_response.raise_for_status()
        
        with open(filename, 'wb') as f:
            for chunk in image_response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"store in: {filename}")
        
    except requests.exceptions.RequestException as e:
        print(f"download err: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"unknown err: {e}")
        sys.exit(1)

if __name__ == "__main__":
    print(sys.argv[10])
    print(sys.argv[11])
    if len(sys.argv) != 13:
        print("usage: python getRenderPng.py <renderUrl> <pngUrl> <bk_app_code> <bk_app_secret> <bk_username> <bk_biz_id> <dashboard_uid> <panel_id> <app> <cluster_domain> <start_time> <end_time>")
        sys.exit(1)
    renderUrl, pngUrl, bk_app_code, bk_app_secret, bk_username, bk_biz_id, dashboard_uid, panel_id, app, cluster_domain, start_time, end_time = sys.argv[1:13]
    bk_biz_id = int(bk_biz_id)
    print(bk_biz_id)
    filename = "result_curve/" + start_time + "-" + end_time + ".jpeg"
    start_time = int(start_time)
    end_time = int(end_time)
    task_id = getRenderTaskId(renderUrl, bk_app_code, bk_app_secret, bk_username, bk_biz_id, dashboard_uid, panel_id, app, cluster_domain, start_time, end_time)
    print(f"task id is {task_id}")
    time.sleep(10)
    image_url = getrenderCurve(pngUrl, bk_app_code, bk_app_secret, bk_username, task_id)
    
    download_image(image_url, filename)
