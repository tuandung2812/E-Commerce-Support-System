import functools
from models.device import DeviceModel
from flask import request


def is_valid(api_key):
    device = DeviceModel.find_by_device_key(device_key = api_key)
    if device:
        return True

def api_required(func):
    @functools.wraps(func)
    def decorator(*args, **kwargs):

        if "api_key" in request.args:
            api_key = request.args["api_key"]
        else:
            return {"message": "Please provide an API key"}, 400
        # Check if API key is correct and valid
        if request.method == "GET" and is_valid(api_key):
            return func(*args, **kwargs)
        else:
            return {"message": "The provided API key is not valid"}, 403
    return decorator
