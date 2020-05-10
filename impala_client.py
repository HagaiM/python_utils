import impala.dbapi
import dataframe_converter_utils

"""
This is the impala client
"""


class ImpalaClient:

    _instance = None
    CHUNK_SIZE = 1000

    # @staticmethod
    # def get_instance(impala_host, impala_port=21050):
    #     """ Static access method. """
    #     if ImpalaClient._instance is None:
    #         ImpalaClient(impala_host, impala_port)
    #     return ImpalaClient._instance
    #
    # def __init__(self, impala_host, impala_port=21050):
    #     """ Virtually private constructor. """
    #     if ImpalaClient._instance is not None:
    #         raise Exception("This class is a singleton!")
    #     else:
    #         self._impala_host = impala_host
    #         self._impala_port = impala_port
    #         self._impala_connect = self._create_impala_connect()
    #         ImpalaClient._instance = self

    def __init__(self, impala_host, impala_port=21050):
        self._impala_host = impala_host
        self._impala_port = impala_port
        self._impala_connect = self._create_impala_connect()

    def _create_impala_connect(self):
        """
        creates an impala client
        :return: an impala client
        """
        if not self._impala_host or not self._impala_port:
            raise Exception('the impala host or port is null and therefore cannot create a client')
        else:
            # Connect to impala  server
            return impala.dbapi.connect(host=self._impala_host, port=self._impala_port)

    def get_df(self,query):
        data, columns_names = self.get(query)
        return dataframe_converter_utils.data_to_df(data, columns_names)

    def get(self, query):
        """
        return the table rows
        :param query (str): a select query
        :return:
        """
        cursor = self._impala_connect.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        # get the result set's schema
        result_schema = cursor.description
        columns_names = [column[0] for column in result_schema]
        cursor.close()
        return data, columns_names

    def close(self):
        self._impala_connect.close()
        ImpalaClient._instance = None

# if __name__ == "__main__":
#     TABLE_NAME = 'process_machine_discrete_measurements_fact_data_science'
#     QUERY = ''
#     host_name = ''
#     data_object = ImpalaClient(host_name)
#     start_date = ''
#     end_date = ''
#     machine_code = ''
#     department = 
#
#     profile_name = ''
#     port = 21050
#     QUERY = ''
#     print(QUERY)
#     data_df = data_object.get(QUERY)
#     print(type(data_df))


