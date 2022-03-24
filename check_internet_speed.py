import time
import sqlite3
import speedtest

from datetime import datetime, timedelta


class internet_speed_calculator():

    def __init__(self):
        self.conn = sqlite3.connect('internet_speed.db')
        self.cursor = self.conn.cursor()
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS internet_speed
                     (upload_speed real, download_speed real, time timestamp)''')
        self.conn.commit()

    def speed_finder(self):
        servers = []
        threads = None
        speed = speedtest.Speedtest()
        speed.get_servers(servers)

        # Finding the nearest & best servers for speedtest
        speed.get_best_server()

        # Upload speed test
        speed.download(threads=threads)

        # Download speed test
        speed.upload(threads=threads)
        speed.results.share()
        results_dict = speed.results.dict()
        print(results_dict)

        # Parse the download speed value
        data = int(results_dict['download']) / 1024
        download = str("%.2f" % round(data, 2))  # + ' Kbps'

        # Parse the upload speed value
        data = int(results_dict['upload']) / 1024
        upload = str("%.2f" % round(data, 2))  # + ' Kbps'

        # Parse the timestamp
        timestamp = results_dict['timestamp'].split('.')[0]

        # Prepare the output dictionary
        output = {'download_speed': download,
                  'upload_speed': upload,
                  'timestamp': timestamp}
        return output

    def store_data(self, data):
        self.cursor.execute("INSERT INTO internet_speed VALUES ({download_speed},{upload_speed},'{timestamp}')".
                            format(download_speed=data["download_speed"], upload_speed=data["upload_speed"],
                                   timestamp=data["timestamp"]))
        self.conn.commit()

    def truncate_historic_data(self):
        date_before_1week = datetime.now() - timedelta(days=7)
        date_before_1week = date_before_1week.strftime("%Y-%m-%dT%H:%M:%SZ")
        self.cursor.execute(
            "DELETE FROM internet_speed where time < '{timestamp}' ".format(timestamp=date_before_1week))
        self.conn.commit()

    def runner(self):
        while True:
            data = self.speed_finder()
            self.store_data(data)
            print("Sleeping for 30 seconds")
            self.truncate_historic_data()
            time.sleep(30)


if __name__ == '__main__':
    speed_find = internet_speed_calculator()
    speed_find.runner()
