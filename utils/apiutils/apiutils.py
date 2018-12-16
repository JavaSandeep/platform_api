from flask import request
from authenticator import Authenticator

class APIUtils:
    __size_limit = None

    def __init__():
        try:
            config_file_path = os.path.join(os.environ('PLATFORM_HOME'), "config", "config.json")
            with open(config_file_path) as f:
                config = json.load(f)
        except Exception as ex:
            print("Could not get configurations. {0}".str(ex))
            sys.exit(-1)

        self.__size_limit = config.get("api").get("size-limit")

    @staticmethod
    def authenticate(fn):
        def wrapper():
            ok = Authenticator().validate_token(request.headers.get('api-token'))
            if not ok:
                __msg = str({
                    "status": "failed",
                    "message": "UNAUTHORIZED USER"
                })
                __status = 401
                return __msg, __status
            return fn()
        return wrapper

    @staticmethod
    def validate_size(fn):
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
    
    def validate_req_size():
        cl = request.content_length
        if cl is not None and cl > self.__size_limit * 1024 * 1024:
            return False
        else:
            return True
