import sys
import requests
import json
import sqlite3
import os.path


class RaceSession:
    def getKeys(self):
        req_url = 'https://api.openf1.org/v1/drivers?session_key=latest'
        response = requests.request("GET", req_url)
        if response.status_code == 200:
            response = response.json()
            sessionKey = response[0]['session_key']
            meetingKey = response[0]['meeting_key']
        else:
            print(response.status_code)

        return sessionKey, meetingKey


    def getSessionDrivers(self, sessionKey):
        req_url = 'https://api.openf1.org/v1/drivers?session_key=' + str(sessionKey)
        response = requests.request("GET", req_url)
        if response.status_code == 200:
            drivers = response.json()
        else:
            print(response.status_code)
        return drivers


def main():

    race = RaceSession()
    keys = race.getKeys()

    sessionKey = keys[0]
    meetingKey = keys[1]
    print()
    print('Session key is: ' + str(sessionKey))
    print('Meeting key is: ' + str(meetingKey))
    print()

    # Fetch drivers for the session
    drivers = race.getSessionDrivers(sessionKey)

    # Create our database, but check if it exists first
    if os.path.exists("race.db"):
        print('Database file already exists!')
    else:
        dbconnection = sqlite3.connect('race.db')
        c = dbconnection.cursor()
        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='drivers' ''')
        # If the count is 1, then table exists
        if c.fetchone()[0] == 1:
            print('Drivers table already exists in the database.')
            dbconnection.close()
        else:
            c.execute("CREATE TABLE drivers (id varchar(3), driver_number, broadcast_name, first_name, last_name, full_name, country_code, team_colour, team_name, headshot_url)")
            for driver in drivers:
                c.execute("insert into drivers values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          [driver['name_acronym'],
                           driver['driver_number'],
                           driver['broadcast_name'],
                           driver['first_name'],
                           driver['last_name'],
                           driver['full_name'],
                           driver['country_code'],
                           driver['team_colour'],
                           driver['team_name'],
                           driver['headshot_url']])
                dbconnection.commit()
            dbconnection.close()
            print("Drivers written to database.")

if __name__ == "__main__":
    sys.exit(main())
