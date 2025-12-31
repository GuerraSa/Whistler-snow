import base64
import requests

def analyze_webcam_image(image_path, api_key):
    # Encode image
    with open(image_path, "rb") as image_file:
        base64_image = base64.b64encode(image_file.read()).decode('utf-8')

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    payload = {
        "model": "gpt-4o",
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Describe the sky condition in one word: Bluebird, Sunny, Cloudy, Overcast, Foggy, or Night."},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                ]
            }
        ],
        "max_tokens": 10
    }

    response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
    return response.json()['choices'][0]['message']['content']


def infer_ski_condition(temp, wind_speed, snow_last_hour, is_daytime=True):
    """
    Infers the skiing condition based on sensor data.
    """
    condition = []

    # 1. Temperature Check (Freezing vs Melting)
    is_freezing = temp <= 0

    # 2. Precipitation Check (Needs a 'snow_last_hour' metric or webcam flag)
    if snow_last_hour > 0:
        if is_freezing:
            condition.append("Snowing")
        else:
            condition.append("Raining/Wet Snow")

    # 3. Wind Check
    if wind_speed > 60:
        condition.append("Stormy/High Winds")
    elif wind_speed > 35:
        condition.append("Windy")

    # 4. Fallback if no precip or wind
    if not condition:
        # Without image analysis, we can't definitively say "Bluebird" vs "Cloudy"
        # But we can default to "Fair" or use a placeholder
        condition.append("Fair/Cloudy (Check Cam)")

    return " & ".join(condition)