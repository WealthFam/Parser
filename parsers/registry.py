# Compatibility with both microservice and backend-style base parsers


class ParserRegistry:
    _sms_parsers = []
    _email_parsers = []

    @classmethod
    def register_sms(cls, parser):
        cls._sms_parsers.append(parser)

    @classmethod
    def register_email(cls, parser):
        cls._email_parsers.append(parser)
    
    @classmethod
    def get_sms_parsers(cls):
        return cls._sms_parsers

    @classmethod
    def get_email_parsers(cls):
        return cls._email_parsers
