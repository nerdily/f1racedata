import sys
import requests
import json
import sqlite3
import os.path

from requests import session


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


    def getMeetingInfo(self, meetingKey):
        req_url = 'https://api.openf1.org/v1/meetings?meeting_key=' + str(meetingKey)
        response = requests.request("GET", req_url)
        if response.status_code == 200:
            meetingInfo = response.json()
        else:
            print(response.status_code)
        return meetingInfo


    def getSessionInfo(self, sessionKey):
        req_url = 'https://api.openf1.org/v1/sessions?session_key=' + str(sessionKey)
        response = requests.request("GET", req_url)
        if response.status_code == 200:
            sessionInfo = response.json()
        else:
            print(response.status_code)
        return sessionInfo


    def getSessionDrivers(self, sessionKey):
        req_url = 'https://api.openf1.org/v1/drivers?session_key=' + str(sessionKey)
        response = requests.request("GET", req_url)
        if response.status_code == 200:
            drivers = response.json()
        else:
            print(response.status_code)
        return drivers


class RaceDatabase:
    def createDatabase(self, db_name):
        if os.path.exists(db_name):
            print('Database file already exists! Quitting.')
        else:
            dbconnection = sqlite3.connect(db_name)
            c = dbconnection.cursor()
            dbconnection.commit()
            print('Database ' + db_name + ' created')


    def createMeetingInfoTable(self, db_name, meeting):
        dbconnection = sqlite3.connect(db_name)
        c = dbconnection.cursor()
        # Check if the meetingInfo table already exists. If it does, warn and move on. Else, create and write it
        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' and NAME='meetingInfo' ''')
        if c.fetchone()[0] == 1:
            print('meetingInfo table already exists in the database!')
        else:
            c.execute("CREATE TABLE meetingInfo (meeting_key, meeting_name, "
                      "meeting_official_name, meeting_code, date_start, gmt_offset, "
                      "circuit_key, circuit_short_name, country_key, country_code, country_name, year)")
            for meeting in meeting:
                c.execute("insert into meetingInfo values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          [meeting['meeting_key'],
                           meeting['meeting_name'],
                           meeting['meeting_official_name'],
                           meeting['meeting_code'],
                           meeting['date_start'],
                           meeting['gmt_offset'],
                           meeting['circuit_key'],
                           meeting['circuit_short_name'],
                           meeting['country_key'],
                           meeting['country_code'],
                           meeting['country_name'],
                           meeting['year']])
                dbconnection.commit()
                print('Meeting info writen into database')
        dbconnection.close()


    def createSessionInfoTable(self, db_name, session):
        dbconnection = sqlite3.connect(db_name)
        c = dbconnection.cursor()
        # Check if the 'sessionInfo' table already exists. If it does, warn and move on. Else, create and write it.
        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' and NAME='sessionInfo' ''')
        if c.fetchone()[0] == 1:
            print('sessionInfo table already exists in the database!')
        else:
            c.execute("CREATE TABLE sessionInfo (session_key, "
                      "meeting_key, date_start, date_end, gmt_offset, "
                      "session_type, location, country_name, country_code, "
                      "circuit_key, circuit_short_name)")
            for session in session:
                c.execute("insert into sessionInfo values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          [session['session_key'],
                           session['meeting_key'],
                           session['date_start'],
                           session['date_end'],
                           session['gmt_offset'],
                           session['session_type'],
                           session['location'],
                           session['country_name'],
                           session['country_code'],
                           session['circuit_key'],
                           session['circuit_short_name']])
                dbconnection.commit()
                print("Session info written to database")
        dbconnection.close()


    def createDriversTable(self, db_name, drivers):
        dbconnection = sqlite3.connect(db_name)
        c = dbconnection.cursor()
        # Check if the 'drivers' table already exists. If it does, warn and move on. Else, create and write it.
        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='drivers' ''')
        if c.fetchone()[0] == 1:
            print('Drivers table already exists in the database.')
        else:
            c.execute(
                "CREATE TABLE drivers (id varchar(3), driver_number, broadcast_name, first_name, last_name, full_name, country_code, team_colour, team_name, headshot_url)")
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
            print("Driver info written to database.")
        dbconnection.close()


def main():
    race = RaceSession()
    # Get and set the session and meeting keys
    keys = race.getKeys()
    sessionKey = keys[0]
    meetingKey = keys[1]

    # Fetch session and meeting info, use session info to set database name
    meeting = race.getMeetingInfo(meetingKey)
    session = race.getSessionInfo(sessionKey)
    db_name = session[0]['circuit_short_name'] + '-' + str(session[0]['year']) + '-' + session[0]['session_name'] + '.db'

    #  Fetch drivers for the session
    drivers = race.getSessionDrivers(sessionKey)

    # Create our database
    database = RaceDatabase()
    database.createDatabase(db_name)

    # Populate meetingInfo, sessionInfo, and Drivers tables
    database.createMeetingInfoTable(db_name, meeting)
    database.createSessionInfoTable(db_name, session)
    database.createDriversTable(db_name, drivers)
if __name__ == "__main__":
    sys.exit(main())
