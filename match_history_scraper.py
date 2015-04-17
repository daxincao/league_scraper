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
        self.teamId = self.get_participant_teamId(matchdata)
        self.participantValues = self.get_participant_values(matchdata)


    def get_participant_summonerName(self, matchdata):
        return str(matchdata['participantIdentities'][self.participantId - 1]['player']['summonerName'])

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
        values = [matchdata['participants'][self.participantId - 1]['stats'][x] for x in colnames]

        # Create dictionary from mapping
        d = dict(zip(colnames, values))
        d['participantId'] = self.participantId
        d['summonerName'] = self.summonerName # Add participantId mapping
        d['teamId'] = self.teamId # Add teamId
        return d

    def get_participant_teamId(self, matchdata):
        return matchdata['participants'][self.participantId-1]['teamId']


# Defines a class for pulling timeline data for each player given their participantId as input

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
        kda_df2 = kda_df[['kill', 'assist', 'death']]
        t =  kda_df[['timestamp']]

        # Create cumulative df
        cum_df = kda_df2.cumsum()

        return pd.concat([t,cum_df], axis=1)

# Get all player stats into a dataframe

def participant_stats_full(matchData):

    # Create stat line for every player
    return pd.DataFrame([participantValues(matchData, i).participantValues for i in range(1,11)])


# Class for scraping all event objects in timeline data
class eventScraper(object):

    def __init__(self, timeLine, match_info):
        self.frames = self.getFrames(timeLine)
        self.stats = participant_stats_full(match_info)
        self.events = self.getEvents()
        self.monsterKills = self.getMonsterKills()
        self.buildingKills = self.getBuildingKills()
        self.championKills = self.getChampionKills()


    def getFrames(self, timeLine):
        return timeLine['frames']

    def getEvents(self):
        return [event for frame in self.frames for event in frame['events']]

    def getMonsterKills(self):
        keep_columns = [ 'timestamp','type','monsterType', 'summonerName', 'teamId', 'position']
        monsterKills = pd.DataFrame([i for i in self.events if i['type']=='ELITE_MONSTER_KILL'])

        # Join MonsterKills with summonerName and teamId
        return pd.merge(monsterKills, self.stats, left_on='killerId', right_on='participantId', how='left')[keep_columns].sort('timestamp')

    def getBuildingKills(self):
        keep_columns = ['timestamp', 'type', 'towerType', 'laneType', 'summonerName', 'teamId', 'position']
        buildingKills = pd.DataFrame([i for i in self.events if i['type'] == 'BUILDING_KILL'])

        # Create lookup frame to avoid duplicate columns names
        lookup_frame = self.stats[['participantId', 'summonerName']]

        # Join events with summoner names
        return pd.merge(
            left=buildingKills,
            right=lookup_frame,
            left_on='killerId',
            right_on='participantId',
            how='left')[keep_columns].sort('timestamp')

    def getChampionKills(self):
        keep_columns = ['timestamp', 'type', 'killerId', 'victimId', 'assistingParticipantIds', 'position']
        championKills = pd.DataFrame([self.lookup_kill_ids(event) for event in self.events if event['type'] == 'CHAMPION_KILL'])
        return championKills[keep_columns]

    def lookupId(self, id_val):
        """
        Helper function to getChampionKills
        :param id_val: id to lookup
        :param lookup_frame: lookup frame
        :return: string
        """

        return self.stats.loc[self.stats['participantId'] == id_val].iloc[0]['summonerName']

    def lookup_kill_ids(self, event):
        '''
        Helper function to getChampionKills
        Replaces all the ids in an event with summoner names
        :param event:
        :return: event
        '''
        new_event = {}
        new_event['killerId'] = self.lookupId(event['killerId'])
        new_event['victimId'] = self.lookupId(event['victimId'])
        new_event['assistingParticipantIds'] = [self.lookupId(id) for id in event['assistingParticipantIds']]
        new_event['timestamp'] = event['timestamp']
        new_event['position'] = event['position']
        new_event['type'] = event['type']

        return new_event



def full_timeline(timeline_data):

    participant_timelines = []

    for i in range(1,11):

        p = participantTimelines(timeline_data, i)

        p_timeline = pd.merge(left=p.participantTimelineDf,
                              right=p.participantKDA,
                              on='timestamp')

        #print p_timeline
        participant_timelines.append(p_timeline)

    return pd.concat(participant_timelines)




if __name__ == '__main__':

    timeline_url = 'https://acs.leagueoflegends.com/v1/stats/game/TRLH1/1001080068/timeline?gameHash=86b3fe64916e9424'

    stat_url = 'https://acs.leagueoflegends.com/v1/stats/game/TRLH1/1001080068?gameHash=86b3fe64916e9424'

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



    # Get timeline data for all players
    timeline_full = full_timeline(timeline_data)

    timeline_full_merged = pd.merge(left=timeline_full,
                                    right=stats_full[['participantId', 'summonerName']],
                                    on='participantId')

    timeline_full_merged.to_csv('sample_timeline.csv', sep=';')


    # Print out events

    test = eventScraper(timeline_data, match_data)

    print test.buildingKills
    print test.monsterKills
    print test.championKills

    order = ['type', 'timestamp', 'summonerName', 'teamId', 'killerId', 'victimId', 'assistingParticipantIds', 'towerType', 'laneType', 'monsterType', 'position']
    pd.concat([test.buildingKills, test.monsterKills, test.championKills])[order].sort('timestamp').to_csv('sample_events.csv', sep=';', index=False)


