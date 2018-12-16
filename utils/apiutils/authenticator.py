class Authenticator:
    def validate_token(self, token):
        return False

    def validate_user_key(self, user_key):
        return None, None

    def decode_token(self):
        pass
    
    __get_user_details_query = """
    """
    