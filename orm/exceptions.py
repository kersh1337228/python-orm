from mysql.connector import Error


class SQLError(Exception):
    def __init__(self, fetched: Error):
        super().__init__(*fetched.args)
        self.message = self.__convert_message(fetched)

    def __convert_message(self, error: Error):
        return {
            -1: 'Unread result found inside of cursor',
            1054: error.msg,
            1062: error.msg,
            1064: 'Syntax error',
            1136: 'INSERT INTO <columns> does not match <values>',
            1146: error.msg
        }[error.errno]

    def __str__(self):
        return self.message
