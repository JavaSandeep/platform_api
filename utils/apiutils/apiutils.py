import os
import sys
import json
import jwt
from datetime import datetime, timedelta

from flask import request
from authenticator import Authenticator

class APIUtils:
    __size_limit = None
    # TOKEN TOL: TIME TO LIVE IN DAYS
    __token_tol = None
    __SECRET = None
    __AlGO = None

    def __init__(self):
        try:
            config_file_path = os.path.join(os.environ('PLATFORM_HOME'), "config", "config.json")
            with open(config_file_path) as f:
                config = json.load(f)
        except Exception as ex:
            print("Could not get configurations. {0}".format(str(ex)))
            sys.exit(-1)

        self.__size_limit = config.get("api").get("size-limit")
        self.__token_tol = config.get("api").get("token-TOL")
        self.__SECRET = config.get("api").get("token").get("secret")
        self.__AlGO = config.get("api").get("token").get("algorithm")

    def authenticate(self, fn):
        def wrapper():
            decoded_token = jwt.decode(request.headers.get('api-token'), 
                                       self.__SECRET, 
                                       algorithm=self.__AlGO)
            ok = Authenticator().validate_token(decoded_token)
            if not ok:
                __msg = str({
                    "status": "failed",
                    "message": "UNAUTHORIZED USER"
                })
                __status = 401
                return __msg, __status
            return fn()
        return wrapper

    def validate_size(self, fn):
        def wrapper():
            ok = self.validate_req_size()
            if not ok:
                __msg = str({
                    "status": "failed",
                    "message": "REQUEST ENTITY TOO LARGE"
                })
                __status = 413
                return __msg, __status    
            return fn()
        return wrapper
    
    def generate_token(self, user_key):
        org_id, user_id = Authenticator().validate_user_key(user_key)
        if not org_id or not user_id:
            __msg = str({
                "status": "failed",
                "message": "UNAUTHORIZED USER"
            })
            __status = 401
            return __msg, __status
        try:
            curr_time = datetime.utcnow()
            token_payload = {
                "user_key": user_key,
                "organization_id": org_id,
                "user_id": user_id,
                "iat": curr_time,
                "exp": curr_time + timedelta(days=self.__token_tol)
            }
            token = jwt.encode(token_payload, 
                               self.__SECRET, 
                               algorithm=self.__AlGO)
        except Exception as ex:
            __msg = str({
                "status": "failed",
                "message": "Method failure"
            })
            __status = 520
            return __msg, __status
        return str({"status": "success", "token": token}), 201

    def validate_req_size(self):
        """
        Function to validate request size for a POST request
        :returns: boolean if true else false
        """
        cl = request.content_length
        if cl is not None and cl > self.__size_limit * 1024 * 1024:
            return False
        else:
            return True
