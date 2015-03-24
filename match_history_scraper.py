import requests
import pandas as pd
import collections


# Defines a class for pulling match information

class gameValues(object):

    def __init__(self, matchdata):
        self.seasonId = matchdata['seasonId']
        self.gameId = matchdata['gameId']
        self.gameType = matchdata['gameType']
        self.platformId = matchdata['platformId']
        self.gameDuration = matchdata['gameDuration']

# Defines a class for pulling cumulative stats for each player given their participantId as input

class participantValues(gameValues):

    def __init__(self, matchdata, id):
        self.participantId = id
        self.summonerName = self.get_participant_summonerName(matchdata)
        self.participantValues = self.get_participant_values(matchdata)

    def get_participant_summonerName(self, matchdata):
        return str(matchdata['participantIdentities'][self.participantId -1]['player']['summonerName'])

    def get_participant_values(self, matchdata):

        """
        Returns a dictionary of stat values
        :rtype : dict
        """
        # Parse data for select fields
        colnames = ['goldEarned',
                    'totalMinionsKilled',
                    'kills',
                    'deaths',
                    'assists'
                    ]
        values = [matchdata['participants'][self.participantId -1]['stats'][x] for x in colnames]

        # Create dictionary from mapping
        d = dict(zip(colnames,values))
        d['participantId'] = self.participantId
        d['summonerName'] = self.summonerName # Add participantId mapping
        return d


# Defines a class for pulling timeline data for each player given their partcipantId as input

class participantTimelines(object):
    
    def __init__(self, timeline, id):
        self.participantId = id
        self.participantTimelineDf = self.get_participantTimeline(timeline['frames'])
        self.participantKDA = self.get_participantKDA_timeline(timeline['frames'])

    def get_participantTimeline(self, frames):

        # Get time series labels
        time_values = [x['timestamp'] for x in frames]
        time_values_df = pd.DataFrame(time_values, columns=['timestamp'])

        # Get other series
        gold_values_df = pd.DataFrame([x['participantFrames'][str(self.participantId)] for x in frames])

        # Return concatenated df
        return pd.concat([time_values_df, gold_values_df], axis=1)

    def get_event_result(self, event):
        '''
        Returns a string containing combat result, or empty string for non-combat events
        :param event:
        :return: string
        '''

        # kda =  {'kills': 0,
        # 'assists' : 0,
        #         'deaths' : 0}

        if event['type'] == 'CHAMPION_KILL':
            if self.participantId == event['killerId']:
                return 'kill'
                #kda['kills'] = 1
            elif self.participantId in event['assistingParticipantIds']:
                return 'assist'
                #kda['assists'] = 1
            elif self.participantId == event['victimId']:
                return 'death'
                #kda['deaths'] = 1
            else:
                return ''

        return ''


    def get_frame_KDA(self, frame):
        '''
        Returns a dictionary containing kda values for a single frame
        :param frame:
        :return: dict
        '''

        frame_kda = collections.Counter({'kill': 0, 'assist': 0, 'death': 0})

        # Take each event in frame events, and sum together kda
        frame_results = [self.get_event_result(event) for event in frame['events']]

        for result in frame_results:
            frame_kda[result] += 1

        # Delete null events
        del frame_kda['']

        # Return kda with timestamp label
        frame_kda['timestamp'] = frame['timestamp']
        return frame_kda

    def get_participantKDA_timeline(self, frames):

        kda = []

        for frame in frames:
            kda.append(self.get_frame_KDA(frame))

        kda_df = pd.DataFrame(kda)
        kda_df2 = kda_df[['kill','assist','death']]
        t =  kda_df[['timestamp']]

        # Create cumulative df
        cum_df = kda_df2.cumsum()

        return pd.concat([t,cum_df], axis=1)

# Get all player stats into a dataframe

def participant_stats_full(matchData):

    # Create stat line for every player
    return pd.DataFrame([participantValues(matchData, i).participantValues for i in range(1,11)])



if __name__ == '__main__':

    timeline_url = 'https://acs.leagueoflegends.com/v1/stats/game/TRLH1/1000360417/timeline?gameHash=33754e5060541a7e'

    stat_url = 'https://acs.leagueoflegends.com/v1/stats/game/TRLH1/1000360417?gameHash=33754e5060541a7e'

    # Get timeline data
    f = requests.get(timeline_url)

    timeline_data = f.json()

    #print participantTimelines(timeline_data, 1).participantTimelineDf

    # Get match final values

    g = requests.get(stat_url)

    match_data = g.json()

    # Print out stats for every player
    stats_full = participant_stats_full(match_data)

    print stats_full

    # Write to csv
    stats_full.to_csv('sample_stats.csv', sep=';')

    # Get sample timeline data for player 1
    p1 = participantTimelines(timeline_data, 1)

    p1_timeline_data_kda = p1.participantKDA

    p1_timeline_data_gold = p1.participantTimelineDf

    timeline_full =  pd.merge(left=p1_timeline_data_gold,right=p1_timeline_data_kda,on='timestamp')

    timeline_full.to_csv('sample_timeline.csv', sep=';')