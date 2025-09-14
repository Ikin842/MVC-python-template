from greenstalk import Client

class BeanstalkConfig:
    def __init__(self, **context):
        self.__host = context['BEANSTALK_HOST']
        self.__port = context['BEANSTALK_PORT']

    def beanstalk_conn(self):
        return Client((
            self.__host, int(self.__port)
        ))
