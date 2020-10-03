# code built off script from the following blog post
# https://medium.com/swlh/using-python-to-connect-to-stravas-api-and-analyse-your-activities-dummies-guide-5f49727aac86

from pprint import pprint as pp
import gspread
import json
from datetime import datetime, timedelta
import csv
import pendulum
import os
import copy
import codecs
import pandas as pd
import requests
import time

# connect to google spreadsheet
# input
# key: key to google sheet
# worksheet: name of worksheet 
# output
# sht1: google sheet object
def connect_to_spreadsheet(key, worksheet):

    # connect to google sheet
    gc = gspread.oauth()

    # open spreadsheet
    sht1 = gc.open_by_key(key).worksheet(worksheet)

    return sht1

# get first date of challenge
# input
# sht: google sheet
# output
# start_date: first date of challenge
def get_init_date(sht):

    start_date = datetime.strptime(sht.acell('D2').value, "%Y-%m-%d").date()

    return start_date

# convert date to epoch time
def convert_to_epoch(input_date):
    return(datetime(input_date.year, input_date.month, input_date.day).timestamp())

# get last day of challenge in epoch time
def get_end_date_epoch(start_date_epoch, nbr_of_weeks):
    sec_in_day = 86400
    days_in_week = 7
    return start_date_epoch + (nbr_of_weeks * days_in_week * sec_in_day)

# input
# sht: google sheet where user information is stored
# output
# users: dictionary of dictionaries with user data from google sheet input
def get_user_info(sht):

    # iterate over tracking numbers
    # first cell with data is E3
    n = 4
    name_cell = 'A' + str(n)
    strava_id_cell = 'B' + str(n)
    plan_cell = 'C' + str(n)
    
    name = sht.acell(name_cell).value
    strava_id = int(sht.acell(strava_id_cell).value)
    plan = int(sht.acell(plan_cell).value)
    
    users = {}
    
    while name != '':
        strava_id = int(sht.acell(strava_id_cell).value)
        plan = int(sht.acell(plan_cell).value)
        user = {'name' :  name, 'plan' : plan, 'value' : 0, 'row_nbr' : n}
        users[strava_id] = user

        n = n+1
        name_cell = 'A' + str(n)
        strava_id_cell = 'B' + str(n)
        plan_cell = 'C' + str(n)
        name = sht.acell(name_cell).value

    return users

# create dictionary of dictionaries keyed on weeks in challenge's scope
# input
# start_date: first week of challenge
# nbr_of_weeks: length of challenge in weeks
# output
# weekly_totals: dictionary of dictionaries keyed on first day of week
def create_weekly_dictionary(start_date, nbr_of_weeks):

    # initialize dictionary
    weekly_totals = {}

    # set weeks
    weekly_date = start_date
    first_date_col_letter = ''
    second_date_col_letter = 'D' # initial date column
    date_col = str(first_date_col_letter) + str(second_date_col_letter)

    i = 0
    letters_per_column_name = 1
    while i < nbr_of_weeks:
        weekly_totals[weekly_date] = {'date_col': date_col}
        weekly_date = weekly_date + timedelta(days=7)

        # update second column letter
        if (second_date_col_letter == 'Y' or second_date_col_letter =='Z'):
            second_date_col_letter = chr(ord(second_date_col_letter)-24)
            if first_date_col_letter == '':
                first_date_col_letter = 'A'
            else:
                first_date_col_letter = chr(ord(first_date_col_letter)-24)
        else:
            second_date_col_letter = chr(ord(second_date_col_letter)+2)

        date_col = str(first_date_col_letter) + str(second_date_col_letter)
        i = i+1

    return weekly_totals

# create dictionary of dictionaries to use for getting weekly activity data
# input
# weekly_dict: a dictionary of dictionaries keyed on weeks in the challenge's scope
# user_dict: dictionary of dictionaries keyed on strava_id, also contains a string value date_col
# output
# wk_dict: dictionary of dictionaries keyed on week; each week contains a dictionary of dictionary keyed on strava_id
# this dictionary is designed to hold user data on a weekly timeframe
def create_weekly_user_dict(weekly_dict, user_dict):

    wk_dict = weekly_dict
    for week in wk_dict:
        for user in user_dict:
                wk_dict[week][user] = copy.deepcopy(user_dict[user])
    return wk_dict

# call strava api to get user activities
# input
# user_dict: list of users to iterate through
# start_date_epoch: timestamp of date (at midnight) when the challenge starts
# end_date_epoch: timestamp of date (at midnight) of the day after the challenge ends
# client_id : client id of strava application
# client_secret : secret key of strava application
# output
# activities: dictionary of dataframes of activities from strava for each user
def get_user_activities_from_strava(user_dict, start_date_epoch, end_date_epoch, client_id, client_secret):

    # create dictionary of user activities
    activities = {}

    # loop over users
    for user in user_dict:

        ## Get the tokens from file to connect to Strava
        with open('strava_tokens_' + str(user) + '.json') as json_file:
            strava_tokens = json.load(json_file)
        ## If access_token has expired then use the refresh_token to get the new access_token
        if strava_tokens['expires_at'] < time.time():
            #Make Strava auth API call with current refresh token
            response = requests.post(
                                url = 'https://www.strava.com/oauth/token',
                                data = {
                                        'client_id': client_id,
                                        'client_secret': client_secret,
                                        'grant_type': 'refresh_token',
                                        'refresh_token': strava_tokens['refresh_token']
                                        }
                            )
            #Save response as json in new variable
            new_strava_tokens = response.json()
            # Save new tokens to file
            with open('strava_tokens.json', 'w') as outfile:
                json.dump(new_strava_tokens, outfile)
            #Use new Strava tokens from now
            strava_tokens = new_strava_tokens

        #Loop through all activities
        page = 1
        url = "https://www.strava.com/api/v3/activities"
        access_token = strava_tokens['access_token']
        ## Create the dataframe ready for the API call to store your activity data
        user_activities = pd.DataFrame(
            columns = [
                    "id",
                    "name",
                    "start_date_local",
                    "type",
                    "distance",
                    "moving_time",
                    "elapsed_time",
                    "total_elevation_gain"
            ]
        )

        while page < 2: # this is to limit the results coming back just in case a user has hundreds of activities in that window

            # get page of activities from Strava
            r = requests.get(url + '?access_token=' + access_token + '&per_page=200' + '&page=' + str(page) + '&after=' + str(start_date_epoch) + '&before=' + str(end_date_epoch))
            r = r.json()

            # if no results then exit loop
            if not r:
                break
        
            # otherwise add new data to dataframe
            for x in range(len(r)):
                user_activities.loc[x + (page-1)*200,'id'] = r[x]['id']
                user_activities.loc[x + (page-1)*200,'name'] = r[x]['name']
                user_activities.loc[x + (page-1)*200,'start_date_local'] = r[x]['start_date_local']
                user_activities.loc[x + (page-1)*200,'type'] = r[x]['type']
                user_activities.loc[x + (page-1)*200,'distance'] = r[x]['distance']
                user_activities.loc[x + (page-1)*200,'moving_time'] = r[x]['moving_time']
                user_activities.loc[x + (page-1)*200,'elapsed_time'] = r[x]['elapsed_time']
                user_activities.loc[x + (page-1)*200,'total_elevation_gain'] = r[x]['total_elevation_gain']

            # increment page
            page += 1

        # add user_activities df to activities dictionary
        activities[user] = user_activities

    return activities

# iterate over users to get their activity history and mileage/elevation counts
# updates weekly_user_dict variable
# input
# weekly_user_dict: dictionary of dictionaries keyed by week then strava_id, contains weekly user data
# user_dict: dictionary of dictionaries keyed on strava_id, used for loop control
def parse_activity_data(weekly_user_dict, user_dict, activities):

    # loop over user ids
    for user in user_dict:
        strava_id = user

        mileage_total = 0
        rides_over_fifty_two_miles = 0
        rides_over_fifty_two_hundred_feet = 0

        activity = activities[user]


        for index, row in activity.iterrows():
            
            if row['type'] == 'Ride' or row['type'] == 'VirtualRide':
                activity_date = datetime.strptime(row['start_date_local'][:10], '%Y-%m-%d')
                week_of = pendulum.instance(activity_date).start_of('week').date()
                activity_mileage = int(float(row['distance'])) # in meters
                activity_elevation = int(float(row['total_elevation_gain'])) # in meters

                # if activity is from outside the challenge scope, break
                if week_of not in weekly_user_dict:
                    break

                plan = weekly_user_dict[week_of][strava_id]['plan']
  
                # get value that will be written to sheet
                if plan == 1: # 52 miles over the course of the week
                    weekly_user_dict[week_of][strava_id]['value'] += activity_mileage*0.000621371192 # convert to miles
                if plan == 2 and activity_mileage >= 83685: # one 52 mile ride during the week (83685 meters)
                    weekly_user_dict[week_of][strava_id]['value'] += 1
                if plan == 3 and activity_elevation >= 1584: # one 5200 ft ride during the week (1584 meters)
                    weekly_user_dict[week_of][strava_id]['value'] += 1

# write weekly user data to google sheet
# input
# sht: google sheet where user information is stored
# weekly_user_dict: dictionary of dictionaries keyed by week then strava_id, contains weekly user data
# user_ct: number of users
def write_to_sheet(sht, weekly_user_dict, user_ct):

    # iterate over weeks
    for week in weekly_user_dict:

        date_col = weekly_user_dict[week]['date_col']

        # iterate over users
        for user in weekly_user_dict[week]:
            if user != 'date_col':

                plan = weekly_user_dict[week][user]['plan']

                # get user's row number
                row_nbr = weekly_user_dict[week][user]['row_nbr']

                # write value to week/user cell if it isn't 0 (to save on API calls)
                if weekly_user_dict[week][user]['value'] > 0:
                    update_cell = str(date_col) + str(row_nbr)
                    value = weekly_user_dict[week][user]['value']

                    sht.update(update_cell, value)


def main():

    # INPUT
    google_sheet_key = '[GOOGLE_SHEET_KEY]'
    google_sheet_name = '[GOOGLE_SHEET_NAME]'
    nbr_of_weeks = 13
    client_id = [CLIENT_ID]
    client_secret = '[CLIENT_SECRET]'

    # connect to google sheet
    sht = connect_to_spreadsheet(google_sheet_key, google_sheet_name)

    # get first date of challenge
    start_date = get_init_date(sht)
    start_date_epoch = convert_to_epoch(start_date)
    end_date_epoch = get_end_date_epoch(start_date_epoch, nbr_of_weeks)

    # create data structures to store user/activity data
    weekly_dict = create_weekly_dictionary(start_date, nbr_of_weeks)
    user_dict = get_user_info(sht)
    weekly_user_dict = create_weekly_user_dict(weekly_dict, user_dict)

    # get user activity from strava
    activities = get_user_activities_from_strava(user_dict, start_date_epoch, end_date_epoch, client_id, client_secret)

    # add weekly values to weekly/user data structure
    parse_activity_data(weekly_user_dict, user_dict, activities)
    
    # update google sheet with challenge values
    write_to_sheet(sht, weekly_user_dict, len(user_dict))


if __name__ == "__main__":
    main()
