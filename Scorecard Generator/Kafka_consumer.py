import threading
import time
from kafka import KafkaConsumer
import re

name_exception_dict = {'Kusal Perera (wk)' : 'MDKJ Perera', 'Thisara Perera' : 'NLTC Perera'}



class Batsman:
    """ class Batsman with all required attributes """

    def __init__(self, name):
        self.bt_name = name
        self.runs = 0
        self.balls = 0
        self.fours = 0
        self.sixes = 0
        self.time = 0
        self.strike_rate = 0
        self.batted = 0
        self.out = 0
        self.fall_of_wicket_runs = 0
        self.fall_of_wicket_no = -1
        self.fall_of_wicket_ball = ''
        self.dismissalType = 'did not bat'


    def calc_strikeRate(self):
        """ function to calculate strike rate of batsman """

        if self.batted == 1 and self.balls > 0:
            self.strike_rate = (self.runs / self.balls) * 100


    def calc_fallOfWicket(self, total_score, total_wickets):
        """ function to calculate fall of wicket
            required because of reverse order of commentary """

        #self.fall_of_wicket_runs = total_score - self.fall_of_wicket_runs
        if self.fall_of_wicket_no != -1:
            self.fall_of_wicket_no = self.fall_of_wicket_no + 1


    def calc_finalState(self):
        """ function to calculate the final state of batsmen """

        if self.batted == 1 and self.out == 0:
            self.dismissalType = 'not out'



class Bowler:
    """ class Bowler with all required attributes """

    def __init__(self, name):
        self.bw_name = name
        self.runs = 0
        self.complete_overs = 0
        self.maiden_overs = 0
        self.economy = 0
        self.wickets = 0
        self.balls = 0
        self.incomplete_over_balls = 0
        self.extras = 0
        self.no_balls = 0
        self.wide_balls = 0
        self.zero_balls = 0
        self.fours = 0
        self.sixes = 0


    def calc_economy(self):
        """ function to calculate economy of bowler """
        if self.complete_overs > 0 or self.balls > 0:
            overs = self.complete_overs + (self.incomplete_over_balls / 6)
            self.economy = self.runs / overs


class TeamScorecard:
    """ class TeamScorecard to store batting
        and bowling scorecard of a team """

    def __init__(self, name):
        self.team_name = name
        self.extra_runs = 0
        self.no_ball_runs = 0
        self.bye_runs = 0
        self.leg_bye_runs = 0
        self.total_runs = 0
        self.wickets = 0
        self.batsmen = {}
        self.bowlers = {}


class MatchScorecard:
    """ class MatchScorecard to generate and store
        TeamScorecard of both teams playing """

    def __init__(self, id, t1, t2):
        self.matchID = id
        self.team1 = TeamScorecard(t1)
        self.team2 = TeamScorecard(t2)

    def getStats(self):
        """ function to read data from the commentary textfile
            and extract the necessary information """

        prev_over = -1
        innings_no = 1
        ball_no_regex = '^(?P<over>\d+).(?P<ball_no>\d)$'                                                                   # "43.2"
        innings_end_regex = '###+'
        ball_info_regex = '(?P<runs>^\s*\d+\s*$)|(?P<wicket>^\s*W\s*$)|(?P<wide>^\s*\d+w\s*$)|(?P<no_ball>^\s*\d+nb\s*$)|(?P<bye>^\s*\d+b\s*$)|(?P<leg_bye>^\s*\d+lb\s*$)'                                                       # "###########"
        ball_extra_info_regex = '^^\s*(?P<bowler_name>[ a-zA-Z]+)\s+to\s+(?P<batsman_name>[ a-zA-Z]+)\s*,\s*(?P<runs_or_out>[\d\(\)a-zA-Z ]+)\s*,\s*(?P<extra_info>[\da-zA-Z ]*).*$'         # "Boult" to "Stokes"," 1 run"," OUT", Holy Moly! It's a freaking tie. We're not done yet. We will have a Super Over to decide the winner of the 2019
        wicket_info_regex = '^(?P<fn>[a-zA-Z]+)\s*(?P<ln>[a-zA-Z]+)\s*(?P<o_info>[&a-zA-Z \/\(\)\[\]]+)\s*(?P<run>[\d]+)\s*\((?P<time>[\d]+m).*$'   #PJ Cummins run out (Udana) 0 (4m 1b 0x4 0x6) SR: 0.00
        run_extract_regex = '^(?P<r>[\d]+).*$'

        wicket_info_pattern = re.compile(wicket_info_regex)

        ball_no_groups = ['over', 'ball_no']
        ball_info_groups = ['runs', 'wicket', 'wide', 'no_ball', 'bye', 'leg_bye']
        ball_extra_info_groups = ['bowler_name', 'batsman_name', 'runs_or_out', 'extra_info']
        wicket_info_regex = ['firstname', 'lastname', 'out_info', 'runs_scored', 'time_played', 'balls_faced', 'fours_hit', 'sixes_hit', 'strike_rate']

        """ initial default bowler object """
        temp_bowler = Bowler('Empty bowler')

        """ list for storing run out players without facing the ball """
        wickets_temp = []

        """ open file and read data """
        try:
            with open('194161007-'+self.matchID+'-commentary-consumed.txt', 'r') as ip_file:
                while(True):
                    try:
                        ip_str = ip_file.readline()
                    except:
                        raise Exception('First readline')
                    
                    #print(ip_str)
                    """ till file is over """
                    if ip_str == '':
                        #print('False')
                        if temp_bowler.bw_name != 'Empty bowler':
                            #print(temp_bowler.bw_name, 1)
                            self.saveBowlerStats(temp_bowler, innings_no, extra_runs)
                        break
                    ip_str = ip_str[:-1]
                    if ip_str == 'ABANDONED':
                        #print('abandoning')
                        return -1111

                    """ extra check for EOF """
                    eof_flag = 1

                    """ find next ball / end of inning / EOF """
                    while(True):

                        """ next ball """
                        if re.search(ball_no_regex, ip_str):
                            break

                        """ end of inning 1 """
                        if re.search(innings_end_regex, ip_str):                # check if inning is over
                            """ save the bowler stats if inning is over  """
                            if temp_bowler.bw_name != 'Empty bowler':
                                #print(temp_bowler.bw_name, 2)
                                self.saveBowlerStats(temp_bowler, innings_no, extra_runs)
                            #print('\n\n')
                            #print(wickets_temp)
                            """ update wicket details for the run out batsman special case """
                            if wickets_temp != []:
                                for wic in wickets_temp:
                                    """ check if name is available now """
                                    bat_name = self.compareNames(self.team1,wic[0],'')
                                    """ if not available create new object """
                                    if bat_name is None:
                                        self.team1.batsmen[wic[0]] = Batsman(wic[0])
                                        self.team1.batsmen[wic[0]].batted = 1
                                        self.team1.batsmen[wic[0]].out = 1
                                        self.team1.batsmen[wic[0]].dismissalType = wic[2]
                                        self.team1.batsmen[wic[0]].time = wic[3]
                                        self.team1.batsmen[wic[0]].fall_of_wicket_runs = wic[1]
                                        self.team1.batsmen[wic[0]].fall_of_wicket_no = wic[4]
                                        self.team1.batsmen[wic[0]].fall_of_wicket_ball = wic[5]
                                    # if available then update
                                    else:
                                        self.team1.batsmen[bat_name].batted = 1
                                        self.team1.batsmen[bat_name].out = 1
                                        self.team1.batsmen[bat_name].dismissalType = wic[2]
                                        self.team1.batsmen[bat_name].time = wic[3]
                                        self.team1.batsmen[bat_name].fall_of_wicket_runs = wic[1]
                                        self.team1.batsmen[bat_name].fall_of_wicket_no = wic[4]
                                        self.team1.batsmen[bat_name].fall_of_wicket_ball = wic[5]

                            """ reset the values / update inning no. """
                            wickets_temp = []
                            innings_no = 2
                            prev_over = -1
                            temp_bowler = Bowler('Empty bowler')


                        #print(ip_str)
                        try:
                            ip_str = ip_file.readline()
                        except:
                            raise Exception('Unknown readline')
                        #print(ip_str, '####')
                        """ end of inning 2 / EOF """
                        if ip_str == '':
                            #print('True')
                            """ save the bowler stats if commentary is over  """
                            #print(temp_bowler)
                            if temp_bowler.bw_name != 'Empty bowler':
                                #print(temp_bowler.bw_name, 3)
                                self.saveBowlerStats(temp_bowler, innings_no, extra_runs)
                            #print('\n\n')
                            #print(wickets_temp)
                            """ update wicket details for the run out batsman special case """
                            if wickets_temp != []:
                                for wic in wickets_temp:
                                    """ check if name is available now """
                                    bat_name = self.compareNames(self.team2,wic[0],'')
                                    """ if not available create new object """
                                    if bat_name is None:
                                        self.team2.batsmen[wic[0]] = Batsman(wic[0])
                                        self.team2.batsmen[wic[0]].batted = 1
                                        self.team2.batsmen[wic[0]].out = 1
                                        self.team2.batsmen[wic[0]].dismissalType = wic[2]
                                        self.team2.batsmen[wic[0]].time = wic[3]
                                        self.team2.batsmen[wic[0]].fall_of_wicket_runs = wic[1]
                                        self.team2.batsmen[wic[0]].fall_of_wicket_no = wic[4]
                                        self.team2.batsmen[wic[0]].fall_of_wicket_ball = wic[5]
                                    # if available then update
                                    else:
                                        self.team2.batsmen[bat_name].batted = 1
                                        self.team2.batsmen[bat_name].out = 1
                                        self.team2.batsmen[bat_name].dismissalType = wic[2]
                                        self.team2.batsmen[bat_name].time = wic[3]
                                        self.team2.batsmen[bat_name].fall_of_wicket_runs = wic[1]
                                        self.team2.batsmen[bat_name].fall_of_wicket_no = wic[4]
                                        self.team2.batsmen[bat_name].fall_of_wicket_ball = wic[5]

                            """ update EOF flag """
                            eof_flag = -1
                            break

                    """ if EOF is found end function """
                    if eof_flag == -1:
                        break

                    """ read ball details """
                    try:
                        ip_run_wicket = ip_file.readline()[:-1]             # read runs or wicket
                    except:
                        raise Exception('run_wicket')
                    #print(ip_run_wicket)
                    try:
                        ip_ball_description = ip_file.readline()[:-1]       # read ball description
                    except:
                        raise Exception('ball_descrip')

                    """ extract important information """
                    try:
                        ball_no = re.search(ball_no_regex, ip_str)                   # find over no. and ball no.
                        ball_info = re.search(ball_info_regex, ip_run_wicket)
                        b_descrip = re.search(ball_extra_info_regex, ip_ball_description)
                        if ball_no == None or ball_info == None or b_descrip == None:
                            raise
                    except:
                        raise Exception('Incomplete Ball')
                    #print(b_descrip)


                    """ check if new over is starting """
                    if prev_over != ball_no['over'] or prev_over == -1:
                        """ update details of last over for corresponding bowler """
                        if temp_bowler.bw_name != 'Empty bowler':
                            #print(temp_bowler.bw_name, 4)
                            self.saveBowlerStats(temp_bowler, innings_no, extra_runs)

                        """ reset temporary over details for new bowler """
                        #print(ip_ball_description)
                        #print(b_descrip)
                        temp_bowler = Bowler(b_descrip['bowler_name'])
                        prev_over = ball_no['over']
                        extra_runs = 0

                    
                    """ extract outcome of ball (run/ out/ no ball / wide/ bye / leg bye) """
                    for k in ball_info_groups:
                        #print(ip_run_wicket)
                        if ball_info[k] != None:
                            ball_type = k
                            break

                    # ball type -> run
                    if ball_type == 'runs':
                        # find the no. of runs scored on the ball
                        r_temp = int(ball_info['runs'])

                        """ update the correct batsman details """
                        if innings_no == 1:
                            # create new batsman object if new name is found
                            if b_descrip['batsman_name'] not in self.team2.batsmen.keys():
                                self.team2.batsmen[b_descrip['batsman_name']] = Batsman(b_descrip['batsman_name'])
                                # set batted = 1 because name found from the commentary
                                self.team2.batsmen[b_descrip['batsman_name']].batted = 1
                            # update batsman runs
                            self.team2.batsmen[b_descrip['batsman_name']].runs += r_temp
                            # update team runs
                            self.team2.total_runs += r_temp
                            # update balls face by the batsman
                            self.team2.batsmen[b_descrip['batsman_name']].balls += 1
                            # update 4's and 6's for boundary
                            if r_temp == 4:
                                self.team2.batsmen[b_descrip['batsman_name']].fours += 1
                            if r_temp == 6:
                                self.team2.batsmen[b_descrip['batsman_name']].sixes += 1

                        """ update the correct batsman details """
                        if innings_no == 2:
                            # create new batsman object if new name is found
                            if b_descrip['batsman_name'] not in self.team1.batsmen.keys():
                                self.team1.batsmen[b_descrip['batsman_name']] = Batsman(b_descrip['batsman_name'])
                                # set batted = 1 because name found from the commentary
                                self.team1.batsmen[b_descrip['batsman_name']].batted = 1
                            # update batsman runs
                            self.team1.batsmen[b_descrip['batsman_name']].runs += r_temp
                            # update team runs
                            self.team1.total_runs += r_temp
                            # update balls face by the batsman
                            self.team1.batsmen[b_descrip['batsman_name']].balls += 1
                            # update 4's and 6's for boundary
                            if r_temp == 4:
                                self.team1.batsmen[b_descrip['batsman_name']].fours += 1
                            if r_temp == 6:
                                self.team1.batsmen[b_descrip['batsman_name']].sixes += 1

                        """ update the bowler details """
                        temp_bowler.runs += r_temp
                        if r_temp == 0:
                            temp_bowler.zero_balls += 1
                        elif r_temp == 4:
                            temp_bowler.fours += 1
                        elif r_temp == 6:
                            temp_bowler.sixes += 1

                    # ball type -> OUT
                    elif ball_type == 'wicket':
                        #print(ip_ball_description)
                        wicket_lines_count = 0
                        while(True):
                            """ find the line with wicket information """
                            try:
                                ip_wicket = ip_file.readline()[:-1]
                            except:
                                raise Exception('wicket')
                            """ ignore blank lines """
                            if len(ip_wicket) == 0:
                                wicket_lines_count += 1
                                if wicket_lines_count > 20:
                                    raise Exception('wicket')
                                continue
                            if wicket_info_pattern.findall(ip_wicket) != None:
                                break
                        # parse the info
                        #print('asdsad' , ip_wicket)
                        wicket_info = wicket_info_pattern.findall(ip_wicket)
                        #print(wicket_info)
                        #print(ip_wicket)
                        #print(wicket_info)
                        #print(innings_no)

                        """Nature of dismissal - catch/bowled/LBW etc. except run out """
                        if 'run out' not in wicket_info[0][2]:
                            #print(wicket_info[0][2])
                            #print(wicket_info)
                            #print(innings_no)

                            """ update the correct batsman details """
                            if innings_no == 1:
                                # create new batsman object if new name is found
                                if b_descrip['batsman_name'] not in self.team2.batsmen.keys():
                                    self.team2.batsmen[b_descrip['batsman_name']] = Batsman(b_descrip['batsman_name'])
                                    # set batted = 1 because name found from the commentary
                                    self.team2.batsmen[b_descrip['batsman_name']].batted = 1
                                # update balls face by the batsman
                                self.team2.batsmen[b_descrip['batsman_name']].balls += 1
                                # set out = 1
                                self.team2.batsmen[b_descrip['batsman_name']].out = 1
                                # store the current total score of team for fall of wicket
                                self.team2.batsmen[b_descrip['batsman_name']].fall_of_wicket_runs = self.team2.total_runs
                                # store the order of dismissal and increment the total no. of wickets for the team
                                self.team2.batsmen[b_descrip['batsman_name']].fall_of_wicket_no = self.team2.wickets
                                self.team2.wickets += 1
                                # store the ball on which wicket fallen
                                self.team2.batsmen[b_descrip['batsman_name']].fall_of_wicket_ball = ball_no['over'] + '.' + ball_no['ball_no'] + ' ov'
                                # store the nature of dismissal
                                #print(b_descrip['batsman_name'], wicket_info[0][2])
                                self.team2.batsmen[b_descrip['batsman_name']].dismissalType = wicket_info[0][2]
                                # store the time played by batsman
                                self.team2.batsmen[b_descrip['batsman_name']].time = wicket_info[0][4]

                            """ update the correct batsman details """
                            if innings_no == 2:
                                # create new batsman object if new name is found
                                if b_descrip['batsman_name'] not in self.team1.batsmen.keys():
                                    self.team1.batsmen[b_descrip['batsman_name']] = Batsman(b_descrip['batsman_name'])
                                    # set batted = 1 because name found from the commentary
                                    self.team1.batsmen[b_descrip['batsman_name']].batted = 1
                                # update balls face by the batsman
                                self.team1.batsmen[b_descrip['batsman_name']].balls += 1
                                # set out = 1
                                self.team1.batsmen[b_descrip['batsman_name']].out = 1
                                # store the current total score of team for fall of wicket
                                self.team1.batsmen[b_descrip['batsman_name']].fall_of_wicket_runs = self.team1.total_runs
                                # store the order of dismissal and increment the total no. of wickets for the team
                                self.team1.batsmen[b_descrip['batsman_name']].fall_of_wicket_no = self.team1.wickets
                                self.team1.wickets += 1
                                # store the ball on which wicket fallen
                                self.team1.batsmen[b_descrip['batsman_name']].fall_of_wicket_ball = ball_no['over'] + '.' + ball_no['ball_no'] + ' ov'
                                # store the nature of dismissal
                                #print(b_descrip['batsman_name'], wicket_info[0][2])
                                self.team1.batsmen[b_descrip['batsman_name']].dismissalType = wicket_info[0][2]
                                # store the time played by batsman
                                self.team1.batsmen[b_descrip['batsman_name']].time = wicket_info[0][4]
                                
                            """ update bowler stats """
                            temp_bowler.zero_balls += 1
                            temp_bowler.wickets += 1


                        # Nature of dismissal - run out
                        else:
                            """ check if any runs were taken before getting out """
                            if 'run' in b_descrip['runs_or_out']:
                                r_temp = int(re.search(run_extract_regex, b_descrip['runs_or_out'])['r'])
                            else:
                                r_temp = 0

                            #print(b_descrip['batsman_name'], r_temp, wicket_info)

                            """ update the correct batsman details """
                            if innings_no == 1:
                                # create new batsman object if new name is found
                                if b_descrip['batsman_name'] not in self.team2.batsmen.keys():
                                    self.team2.batsmen[b_descrip['batsman_name']] = Batsman(b_descrip['batsman_name'])
                                    # set batted = 1 because name found from the commentary
                                    self.team2.batsmen[b_descrip['batsman_name']].batted = 1
                                # update balls faced by batsman
                                self.team2.batsmen[b_descrip['batsman_name']].balls += 1
                                # update batsman runs
                                self.team2.batsmen[b_descrip['batsman_name']].runs += r_temp
                                
                                """ look for name of run out batsman in the list of batsmen """
                                batsman_out = self.compareNames(self.team2, wicket_info[0][0], wicket_info[0][1])
                                #print(batsman_out)
                                """ if not found store the wicket details for later use
                                    (Special case of wicket) getting run out while at non-strikers end """
                                if batsman_out == None:
                                    wicket_details = []
                                    #print(wicket_info)
                                    wicket_details.append(wicket_info[0][0]+' '+wicket_info[0][1])
                                    wicket_details.append(self.team2.total_runs)
                                    wicket_details.append(wicket_info[0][2])
                                    wicket_details.append(wicket_info[0][4])
                                    wicket_details.append(self.team2.wickets)
                                    self.team2.wickets += 1
                                    #print('asa', ip_str, 'ASJkns')
                                    wicket_details.append(ball_no['over'] + '.' + ball_no['ball_no'] + ' ov')
                                    wickets_temp.append(wicket_details)
                                # if batsman is found update it's details
                                else:
                                    # set out = 1
                                    self.team2.batsmen[batsman_out].out = 1
                                    # store the current total score of team for fall of wicket
                                    self.team2.batsmen[batsman_out].fall_of_wicket_runs = self.team2.total_runs
                                    # store the order of dismissal and incremeent the total no. of wickets for the team
                                    self.team2.batsmen[b_descrip['batsman_name']].fall_of_wicket_no = self.team2.wickets
                                    self.team2.wickets += 1
                                    # store the ball on which wicket fallen
                                    self.team2.batsmen[b_descrip['batsman_name']].fall_of_wicket_ball = ball_no['over'] + '.' + ball_no['ball_no'] + ' ov'
                                    # store the nature of dismissal
                                    self.team2.batsmen[batsman_out].dismissalType = wicket_info[0][2]
                                    # store the time played by batsman
                                    self.team2.batsmen[batsman_out].time = wicket_info[0][4]

                                # update team runs
                                self.team2.total_runs += r_temp

                                
                            """ update the correct batsman details """
                            if innings_no == 2:
                                # create new batsman object if new name is found
                                if b_descrip['batsman_name'] not in self.team1.batsmen.keys():
                                    self.team1.batsmen[b_descrip['batsman_name']] = Batsman(b_descrip['batsman_name'])
                                    # set batted = 1 because name found from the commentary
                                    self.team1.batsmen[b_descrip['batsman_name']].batted = 1
                                # update balls faced by batsman
                                self.team1.batsmen[b_descrip['batsman_name']].balls += 1
                                # update batsman runs
                                self.team1.batsmen[b_descrip['batsman_name']].runs += r_temp
                                
                                """ look for name of run out batsman in the list of batsmen """
                                batsman_out = self.compareNames(self.team1, wicket_info[0][0], wicket_info[0][1])
                                """ if not found store the wicket details for later use
                                    (Special case of wicket) getting run out while at non-strikers end """
                                if batsman_out == None:
                                    wicket_details = []
                                    wicket_details.append(wicket_info[0][0]+' '+wicket_info[0][1])
                                    wicket_details.append(self.team1.total_runs)
                                    wicket_details.append(wicket_info[0][2])
                                    wicket_details.append(wicket_info[0][4])
                                    wicket_details.append(self.team1.wickets)
                                    self.team1.wickets += 1
                                    #print('asa', ip_str, 'ASJkns')
                                    wicket_details.append(ball_no['over'] + '.' + ball_no['ball_no'] + ' ov')
                                    wickets_temp.append(wicket_details)
                                # if batsman is found update it's details
                                else:
                                    # set out = 1
                                    self.team1.batsmen[batsman_out].out = 1
                                    # store the current total score of team for fall of wicket
                                    self.team1.batsmen[batsman_out].fall_of_wicket_runs = self.team1.total_runs
                                    # store the order of dismissal and incremeent the total no. of wickets for the team
                                    self.team1.batsmen[b_descrip['batsman_name']].fall_of_wicket_no = self.team1.wickets
                                    self.team1.wickets += 1
                                    # store the ball on which wicket fallen
                                    self.team1.batsmen[b_descrip['batsman_name']].fall_of_wicket_ball = ball_no['over'] + '.' + ball_no['ball_no'] + ' ov'
                                    # store the nature of dismissal
                                    self.team1.batsmen[batsman_out].dismissalType = wicket_info[0][2]
                                    # store the time played by batsman
                                    self.team1.batsmen[batsman_out].time = wicket_info[0][4]

                                # update team runs
                                self.team1.total_runs += r_temp
                                
                            """ update bowler stats """
                            temp_bowler.runs += r_temp
                            if r_temp == 0:
                                temp_bowler.zero_balls += 1

                    # ball type -> WIDE
                    elif ball_type == 'wide':
                        # find the no. of runs scored on the ball
                        r_temp = int(re.search(run_extract_regex,ball_info['wide'])['r'])

                        """ update the team total score batsman does not get the credits """
                        if innings_no == 1:
                            self.team2.total_runs += r_temp
                        elif innings_no == 2:
                            self.team1.total_runs += r_temp

                        """ update bowler stats """
                        temp_bowler.runs += r_temp
                        temp_bowler.extras += r_temp
                        extra_runs += r_temp
                        temp_bowler.wide_balls += 1

                    # ball type -> NO BALL
                    elif ball_type == 'no_ball':
                        # find the no. of runs scored on the ball
                        r_temp = int(re.search(run_extract_regex,ball_info['no_ball'])['r'])

                        """ update the correct batsman details because batsman can hit runs on no ball """
                        if innings_no == 1:
                            # create new batsman object if new name is found
                            if b_descrip['batsman_name'] not in self.team2.batsmen.keys():
                                self.team2.batsmen[b_descrip['batsman_name']] = Batsman(b_descrip['batsman_name'])
                                # set batted = 1 because name found from the commentary
                                self.team2.batsmen[b_descrip['batsman_name']].batted = 1
                            # update batsman runs, -1 becuse 1 run is awarded for no ball
                            self.team2.batsmen[b_descrip['batsman_name']].runs += r_temp - 1
                            # update the team total score
                            self.team2.total_runs += r_temp
                            # update the team no ball runs
                            self.team2.no_ball_runs += r_temp
                            # update balls faced by batsman
                            self.team2.batsmen[b_descrip['batsman_name']].balls += 1
                            # update 4's and 6's for boundary
                            if r_temp == 5:
                                self.team2.batsmen[b_descrip['batsman_name']].fours += 1
                            if r_temp == 7:
                                self.team2.batsmen[b_descrip['batsman_name']].sixes += 1

                        """ update the correct batsman details because batsman can hit runs on no ball """
                        if innings_no == 2:
                            # create new batsman object if new name is found
                            if b_descrip['batsman_name'] not in self.team1.batsmen.keys():
                                self.team1.batsmen[b_descrip['batsman_name']] = Batsman(b_descrip['batsman_name'])
                                # set batted = 1 because name found from the commentary
                                self.team1.batsmen[b_descrip['batsman_name']].batted = 1
                            # update batsman runs, -1 becuse 1 run is awarded for no ball
                            self.team1.batsmen[b_descrip['batsman_name']].runs += r_temp - 1
                            # update the team total score
                            self.team1.total_runs += r_temp
                            # update the team no ball runs
                            self.team1.no_ball_runs += 1
                            # update balls faced by batsman
                            self.team1.batsmen[b_descrip['batsman_name']].balls += 1
                            # update 4's and 6's for boundary
                            if r_temp == 5:
                                self.team1.batsmen[b_descrip['batsman_name']].fours += 1
                            if r_temp == 7:
                                self.team1.batsmen[b_descrip['batsman_name']].sixes += 1

                        """ update the bowler stats """
                        temp_bowler.runs += r_temp
                        temp_bowler.extras += r_temp
                        extra_runs += 1
                        temp_bowler.no_balls += 1
                        # update boundaries
                        if r_temp == 5:
                            temp_bowler.fours += 1
                        if r_temp == 7:
                            temp_bowler.sixes += 1

                    # ball type -> bye
                    elif ball_type == 'bye':
                        # find the no. of runs scored on the ball
                        r_temp = int(re.search(run_extract_regex,ball_info['bye'])['r'])

                        """ update the correct batsman details """
                        if innings_no == 2:
                            # create new batsman object if new name is found
                            if b_descrip['batsman_name'] not in self.team1.batsmen.keys():
                                self.team1.batsmen[b_descrip['batsman_name']] = Batsman(b_descrip['batsman_name'])
                                # set batted = 1 because name found from the commentary
                                self.team1.batsmen[b_descrip['batsman_name']].batted = 1
                            # update balls faced by batsman
                            self.team1.batsmen[b_descrip['batsman_name']].balls += 1
                            # update the team total score
                            self.team1.total_runs += r_temp
                            # update the team bye runs
                            self.team1.bye_runs += r_temp


                        """ update the correct batsman details """
                        if innings_no == 1:
                            # create new batsman object if new name is found
                            if b_descrip['batsman_name'] not in self.team2.batsmen.keys():
                                self.team2.batsmen[b_descrip['batsman_name']] = Batsman(b_descrip['batsman_name'])
                                # set batted = 1 because name found from the commentary
                                self.team2.batsmen[b_descrip['batsman_name']].batted = 1
                            # update balls faced by batsman
                            self.team2.batsmen[b_descrip['batsman_name']].balls += 1
                            # update the team total score
                            self.team2.total_runs += r_temp
                            # update the team bye runs
                            self.team2.bye_runs += r_temp

                        """ update the bowler stats """
                        temp_bowler.zero_balls += 1
                        extra_runs += r_temp

                    # ball type -> leg bye
                    elif ball_type == 'leg_bye':
                        # find the no. of runs scored on the ball
                        r_temp = int(re.search(run_extract_regex,ball_info['leg_bye'])['r'])

                        """ update the correct batsman details """
                        if innings_no == 2:
                            # create new batsman object if new name is found
                            if b_descrip['batsman_name'] not in self.team1.batsmen.keys():
                                self.team1.batsmen[b_descrip['batsman_name']] = Batsman(b_descrip['batsman_name'])
                                # set batted = 1 because name found from the commentary
                                self.team1.batsmen[b_descrip['batsman_name']].batted = 1
                            # update balls faced by batsman
                            self.team1.batsmen[b_descrip['batsman_name']].balls += 1
                            # update the team total score
                            self.team1.total_runs += r_temp
                            # update the team bye runs
                            self.team1.leg_bye_runs += r_temp


                        """ update the correct batsman details """
                        if innings_no == 1:
                            # create new batsman object if new name is found
                            if b_descrip['batsman_name'] not in self.team2.batsmen.keys():
                                self.team2.batsmen[b_descrip['batsman_name']] = Batsman(b_descrip['batsman_name'])
                                # set batted = 1 because name found from the commentary
                                self.team2.batsmen[b_descrip['batsman_name']].batted = 1
                            # update balls faced by batsman
                            self.team2.batsmen[b_descrip['batsman_name']].balls += 1
                            # update the team total score
                            self.team2.total_runs += r_temp
                            # update the team bye runs
                            self.team2.leg_bye_runs += r_temp

                        """ update the bowler stats """
                        temp_bowler.zero_balls += 1
                        extra_runs += r_temp
                    """ store the last ball no. bowled by bowler in an over
                        used to calculate incomplete overs """
                    
                    if temp_bowler.balls < int(ball_no['ball_no']):
                        temp_bowler.balls = int(ball_no['ball_no'])
                        #print('Huuuuuuu')

        except Exception as e:
            print(e.args)
            #print(temp_bowler.bw_name)
            if temp_bowler.bw_name != 'Empty bowler':
                #print(temp_bowler.bw_name, 5)
                self.saveBowlerStats(temp_bowler, innings_no, extra_runs)
            pass

        for bt in self.team1.batsmen.keys():
            self.team1.batsmen[bt].calc_strikeRate()
            self.team1.batsmen[bt].calc_fallOfWicket(self.team1.total_runs, self.team1.wickets)
            self.team1.batsmen[bt].calc_finalState()

        for bw in self.team1.bowlers.keys():
            self.team1.bowlers[bw].calc_economy()

        for bt in self.team2.batsmen.keys():
            self.team2.batsmen[bt].calc_strikeRate()
            self.team2.batsmen[bt].calc_fallOfWicket(self.team2.total_runs, self.team2.wickets)
            self.team2.batsmen[bt].calc_finalState()

        for bw in self.team2.bowlers.keys():
            self.team2.bowlers[bw].calc_economy()


        return


    def saveBowlerStats(self, temp_bowler, innings_no, extra_runs):
        """ function to save bowler stats at the end of each over """

        """ update the correct batsman details """
        if innings_no == 2:
            # update the extra runs for the batting team
            self.team1.extra_runs += extra_runs
            # create new bowler object if new name is found
            if temp_bowler.bw_name not in self.team2.bowlers.keys():
                #print('Creating new bowler team 2 : ', temp_bowler.bw_name)
                self.team2.bowlers[temp_bowler.bw_name] = temp_bowler
                # if the bowler bowled entire over
                if temp_bowler.balls == 6:
                    # update maiden over count if no runs were given
                    if temp_bowler.runs == 0:
                        self.team2.bowlers[temp_bowler.bw_name].maiden_overs += 1
                    # update complete overs count
                    self.team2.bowlers[temp_bowler.bw_name].complete_overs += 1
                # update no. of balls of incomplete over
                else:
                    self.team2.bowlers[temp_bowler.bw_name].incomplete_over_balls = temp_bowler.balls
            # if bowler object is already present update the stats
            else:
                self.team2.bowlers[temp_bowler.bw_name].runs += temp_bowler.runs
                self.team2.bowlers[temp_bowler.bw_name].wickets += temp_bowler.wickets
                self.team2.bowlers[temp_bowler.bw_name].extras += temp_bowler.extras
                self.team2.bowlers[temp_bowler.bw_name].no_balls += temp_bowler.no_balls
                self.team2.bowlers[temp_bowler.bw_name].wide_balls += temp_bowler.wide_balls
                self.team2.bowlers[temp_bowler.bw_name].zero_balls += temp_bowler.zero_balls
                self.team2.bowlers[temp_bowler.bw_name].fours += temp_bowler.fours
                self.team2.bowlers[temp_bowler.bw_name].sixes += temp_bowler.sixes
                # if the bowler bowled entire over
                if temp_bowler.balls == 6:
                    if temp_bowler.runs == 0:
                        # update maiden over count if no runs were given
                        self.team2.bowlers[temp_bowler.bw_name].maiden_overs += 1
                    # update complete over count
                    self.team2.bowlers[temp_bowler.bw_name].complete_overs += 1
                # update no. of balls of incomplete over
                else:
                    self.team2.bowlers[temp_bowler.bw_name].incomplete_over_balls = temp_bowler.balls

        """ update the correct batsman details """
        if innings_no == 1:
            # update the extra runs for the batting team
            self.team2.extra_runs += extra_runs
            # create new bowler object if new name is found
            if temp_bowler.bw_name not in self.team1.bowlers.keys():
                #print('Creating new bowler team 1 : ', temp_bowler.bw_name)
                self.team1.bowlers[temp_bowler.bw_name] = temp_bowler
                # if the bowler bowled entire over
                if temp_bowler.balls == 6:
                    # update maiden over count if no runs were given
                    if temp_bowler.runs == 0:
                        self.team1.bowlers[temp_bowler.bw_name].maiden_overs += 1
                    # update complete overs count
                    self.team1.bowlers[temp_bowler.bw_name].complete_overs += 1
                # update no. of balls of incomplete over
                else:
                    self.team1.bowlers[temp_bowler.bw_name].incomplete_over_balls = temp_bowler.balls
            # if bowler object is already present update the stats
            else:
                self.team1.bowlers[temp_bowler.bw_name].runs += temp_bowler.runs
                self.team1.bowlers[temp_bowler.bw_name].wickets += temp_bowler.wickets
                self.team1.bowlers[temp_bowler.bw_name].extras += temp_bowler.extras
                self.team1.bowlers[temp_bowler.bw_name].no_balls += temp_bowler.no_balls
                self.team1.bowlers[temp_bowler.bw_name].wide_balls += temp_bowler.wide_balls
                self.team1.bowlers[temp_bowler.bw_name].zero_balls += temp_bowler.zero_balls
                self.team1.bowlers[temp_bowler.bw_name].fours += temp_bowler.fours
                self.team1.bowlers[temp_bowler.bw_name].sixes += temp_bowler.sixes
                # if the bowler bowled entire over
                if temp_bowler.balls == 6:
                    # update maiden over count if no runs were given
                    if temp_bowler.runs == 0:
                        self.team1.bowlers[temp_bowler.bw_name].maiden_overs += 1
                    # update complete overs count
                    self.team1.bowlers[temp_bowler.bw_name].complete_overs += 1
                # update no. of balls of incomplete over
                else:
                    self.team1.bowlers[temp_bowler.bw_name].incomplete_over_balls = temp_bowler.balls



    def compareNames(self, team, fname, lname):
        """ function to compare batsmen names from team list and commentary text """

        #print(team.batsmen.keys())
        """ create regex for all batsmen names stored in
            batsmen list of team and compare with the given name
            and return the matching string otherwise return None """

        for bt_name in team.batsmen.keys():
            b1 = bt_name.upper()
            b2 = bt_name.lower()
            name_regex = '.*'
            for i in range(len(bt_name)):
                name_regex += '['
                name_regex += b1[i]
                name_regex += b2[i]
                name_regex += '].*'

            # batsman name = HM Amla
            # name_regex = .*[Hh].*[Mm].*[Aa].*[Mm].*[Ll].*[Aa].*
            # fname+lname = HashimAmla
            if re.search(name_regex, fname + lname):
                return bt_name

        return None

    def compareBowlerNames(self, team, fname, lname):
        """ function to compare bowler names from team list and commentary text """

        """ create regex for all bowler names stored in
            batsmen list of team and compare with the given name
            and return the matching string otherwise return None """

        for bw_name in team.bowlers.keys():
            b1 = bw_name.upper()
            b2 = bw_name.lower()
            name_regex = '.*'
            for i in range(len(bw_name)):
                name_regex += '['
                name_regex += b1[i]
                name_regex += b2[i]
                name_regex += '].*'

            # batsman name = HM Amla
            # name_regex = .*[Hh].*[Mm].*[Aa].*[Mm].*[Ll].*[Aa].*
            # fname+lname = HashimAmla
            if re.search(name_regex, fname + lname):
                return bw_name

        return None

    def updatePlayerNames(self):
        team_regex = '^.*Team\s*(?P<team_no>\d)\s*:\s*(?P<players>[a-zA-Z \(\),\$\&\!]*).*$'
        #print(self.team1.batsmen.keys())
        #print(self.team2.batsmen.keys())
        
        try:
            with open('194161007-'+ self.matchID + '-commentary.txt', 'r') as ip_file:
                try:
                    t1 = ip_file.readline()[:-1]
                    t2 = ip_file.readline()[:-1]
                except:
                    raise Exception('Team')
                #print(t1)
                #print(t2)
                t1 = re.search(team_regex, t1)
                t2 = re.search(team_regex, t2)
                #print(t1)
                #print(t2)
                for i in t1['players'].split(','):
                    #print(i)
                    #print(self.team2.batsmen.keys())
                    i = i.strip()
                    i = i.split('!')
                    bw_name = None
                    
                    if i[0] in name_exception_dict.keys():
                        bat_name = name_exception_dict[i[0]]
                        bw_name = name_exception_dict[i[0]]
                    else:
                        bat_name = self.compareNames(self.team2, i[0], '')
                        bw_name = self.compareBowlerNames(self.team2, i[0], '')

                    #print(bat_name)
                    #print(bw_name)
                    if bat_name != None:
                        try:
                            self.team2.batsmen[bat_name].bt_name = i[1]
                        except:
                            self.team2.batsmen[i[1]] = Batsman(i[1])
                    else:
                        #print('none', i[0], i)
                        self.team2.batsmen[i[1]] = Batsman(i[1])

                    if bw_name != None and bw_name in self.team2.bowlers.keys():
                        self.team2.bowlers[bw_name].bw_name = i[1]



                for i in t2['players'].split(','):
                    #print(i)
                    #print(self.team1.batsmen.keys())
                    i = i.strip()
                    i = i.split('!')
                    bw_name = None
                    if i[0] in name_exception_dict.keys():
                        #print(name_exception_dict[i[0]])
                        bat_name = name_exception_dict[i[0]]
                        bw_name = name_exception_dict[i[0]]
                    else:
                        bat_name = self.compareNames(self.team1, i[0], '')
                        bw_name = self.compareBowlerNames(self.team1, i[0], '')

                    if bat_name != None:
                        try:
                            self.team1.batsmen[bat_name].bt_name = i[1]
                        except:
                            self.team1.batsmen[i[1]] = Batsman(i[1])
                    else:
                        #print(i)
                        self.team1.batsmen[i[1]] = Batsman(i[1])

                    if bw_name != None and bw_name in self.team1.bowlers.keys():
                        self.team1.bowlers[bw_name].bw_name = i[1]
        except Exception as e:
            print(e.args)
            pass


    def printStats(self):
        """ function to print the final output """

        op_file = open('194161007-'+ self.matchID + '-scorecard-computed.txt', 'w')
        batsmen_attributes = ['bt_name', 'dismissalType', 'runs', 'balls', 'time', 'fours', 'sixes', 'strike_rate']

        team1_fall_of_wicket = {}
        team1_did_not_bat = []

        team2_fall_of_wicket = {}
        team2_did_not_bat = []

        for bt in self.team2.batsmen.keys():
            if self.team2.batsmen[bt].fall_of_wicket_no == -1:
                continue
            team2_fall_of_wicket[self.team2.batsmen[bt].fall_of_wicket_no] = (self.team2.batsmen[bt].bt_name, self.team2.batsmen[bt].fall_of_wicket_no, self.team2.batsmen[bt].fall_of_wicket_runs, self.team2.batsmen[bt].fall_of_wicket_ball)

        for bt in self.team2.batsmen.keys():
            if self.team2.batsmen[bt].batted == 0:
                team2_did_not_bat.append(self.team2.batsmen[bt].bt_name)

        for bt in self.team1.batsmen.keys():
            if self.team1.batsmen[bt].fall_of_wicket_no == -1:
                continue
            team1_fall_of_wicket[self.team1.batsmen[bt].fall_of_wicket_no] = (self.team1.batsmen[bt].bt_name, self.team1.batsmen[bt].fall_of_wicket_no, self.team1.batsmen[bt].fall_of_wicket_runs, self.team1.batsmen[bt].fall_of_wicket_ball)

        for bt in self.team1.batsmen.keys():
            if self.team1.batsmen[bt].batted == 0:
                team1_did_not_bat.append(self.team1.batsmen[bt].bt_name)

        #print('Innings 1 Scorecard\n------------------')
        op_file.write('Inning 1 Scorecard\n------------------\n')
        #print('Batting Scorecard')
        op_file.write('Batting Scorecard\n')
        #print('Batsmen\t\t\t\tR\tB\tM\t4s\t6s\tSR')
        #print('{0:<30}\t{1:<30}\t{2:^3}\t{3:^3}\t{4:^4}\t{5:^3}\t{6:^3}\t{7:^5}'.format('BATSMEN', '', 'R', 'B', 'M', '4s', '6s', 'SR'))
        op_file.write('{0:<30}\t{1:<30}\t{2:^3}\t{3:^3}\t{4:^4}\t{5:^3}\t{6:^3}\t{7:^5}\n'.format('BATSMEN', '', 'R', 'B', 'M', '4s', '6s', 'SR'))
        #print(self.team2.batsmen.keys())
        for bt in self.team2.batsmen.keys():
            if self.team2.batsmen[bt].dismissalType == 'did not bat':
                #print(self.team2.batsmen[bt].bt_name, self.team2.batsmen[bt].dismissalType)
                continue
            #print('{0:<30}\t{1:<30}\t{2:^3}\t{3:^3}\t{4:^4}\t{5:^3}\t{6:^3}\t{7:.2f}'.format(
            #                                self.team2.batsmen[bt].bt_name, self.team2.batsmen[bt].dismissalType, self.team2.batsmen[bt].runs,
            #                                self.team2.batsmen[bt].balls, self.team2.batsmen[bt].time, self.team2.batsmen[bt].fours,
            #                                self.team2.batsmen[bt].sixes, self.team2.batsmen[bt].strike_rate))
            op_file.write('{0:<30}\t{1:<30}\t{2:^3}\t{3:^3}\t{4:^4}\t{5:^3}\t{6:^3}\t{7:.2f}\n'.format(
                                            self.team2.batsmen[bt].bt_name, self.team2.batsmen[bt].dismissalType, self.team2.batsmen[bt].runs,
                                            self.team2.batsmen[bt].balls, self.team2.batsmen[bt].time, self.team2.batsmen[bt].fours,
                                            self.team2.batsmen[bt].sixes, self.team2.batsmen[bt].strike_rate))


        wide_runs = self.team2.extra_runs - self.team2.leg_bye_runs - self.team2.bye_runs - self.team2.no_ball_runs
        #print('Extras {0:<4} ( lb {1:>2}, b {2:>2}, nb {3:>2}, w {4:>2} )'.format(self.team2.extra_runs, self.team2.leg_bye_runs, self.team2.bye_runs, self.team2.no_ball_runs, wide_runs))
        op_file.write('Extras {0:<4} ( lb {1:>2}, b {2:>2}, nb {3:>2}, w {4:>2} )\n'.format(self.team2.extra_runs, self.team2.leg_bye_runs, self.team2.bye_runs, self.team2.no_ball_runs, wide_runs))
        #print('TOTAL {0:4}'.format(self.team2.total_runs))
        op_file.write('TOTAL {0:4}/{1:<2}\n'.format(self.team2.total_runs,self.team2.wickets))

        #print('Did not bat: ')
        op_file.write('Did not bat: ')
        did_not_bat_str = ''
        for i in range(len(team2_did_not_bat)):
            did_not_bat_str += (team2_did_not_bat[i] + ', ')
        #print(did_not_bat_str)
        op_file.write(did_not_bat_str + '\n')

        #print('Fall of wickets: ')
        op_file.write('Fall of wickets: ')
        fall_str = ''
        for i in sorted(team2_fall_of_wicket.keys()):
            fall_str += str(team2_fall_of_wicket[i][1])
            fall_str += ('-')
            fall_str += str(team2_fall_of_wicket[i][2])
            fall_str += ' ({0}, {1}), '.format(team2_fall_of_wicket[i][0], team2_fall_of_wicket[i][3])
            
        #print(fall_str)
        op_file.write(fall_str + '\n')





        #print('\nBowling Scorecard')
        op_file.write('\nBowling Scorecard\n')
        #print('{0:<30}  {1:^4}  {2:^3}  {3:^3}  {4:^4}  {5:^6}  {6:^3}  {7:^5}  {8:^3}  {9:^3}  {10:^3}'.format('Bowler', 'O', 'M', 'R', 'W', 'Econ', '0s', '4s', '6s', 'WD', 'NB'))
        op_file.write('{0:<30}  {1:^4}  {2:^3}  {3:^3}  {4:^4}  {5:^6}  {6:^3}  {7:^5}  {8:^3}  {9:^3}  {10:^3}\n'.format('Bowler', 'O', 'M', 'R', 'W', 'Econ', '0s', '4s', '6s', 'WD', 'NB'))
        for bw in self.team1.bowlers.keys():
            #print('{0:<30}  {1:^4}  {2:^3}  {3:^3}  {4:^4}  {5:^6}  {6:^3}  {7:^5}  {8:^3}  {9:^3}  {10:^3}'.format(
            #                                self.team1.bowlers[bw].bw_name, self.team1.bowlers[bw].complete_overs + (self.team1.bowlers[bw].incomplete_over_balls / 10),
            #                                self.team1.bowlers[bw].maiden_overs, self.team1.bowlers[bw].runs, self.team1.bowlers[bw].wickets,
            #                                '{0:.2f}'.format(self.team1.bowlers[bw].economy), self.team1.bowlers[bw].zero_balls, self.team1.bowlers[bw].fours,
            #                                self.team1.bowlers[bw].sixes, self.team1.bowlers[bw].wide_balls, self.team1.bowlers[bw].no_balls))
            op_file.write('{0:<30}\t{1:^4}\t{2:^3}  {3:^3}  {4:^4}  {5:^6}  {6:^3}  {7:^5}  {8:^3}  {9:^3}  {10:^3}\n'.format(
                                            self.team1.bowlers[bw].bw_name, self.team1.bowlers[bw].complete_overs + (self.team1.bowlers[bw].incomplete_over_balls / 10),
                                            self.team1.bowlers[bw].maiden_overs, self.team1.bowlers[bw].runs, self.team1.bowlers[bw].wickets,
                                            '{0:.2f}'.format(self.team1.bowlers[bw].economy), self.team1.bowlers[bw].zero_balls, self.team1.bowlers[bw].fours,
                                            self.team1.bowlers[bw].sixes, self.team1.bowlers[bw].wide_balls, self.team1.bowlers[bw].no_balls))


        
        #print('\n\nInnings 2 Scorecard\n------------------')
        op_file.write('\n\nInnings 2 Scorecard\n------------------\n')
        #print('Batting Scorecard')
        op_file.write('Batting Scorecard\n')
        #print('Batsmen\t\t\t\tR\tB\tM\t4s\t6s\tSR')
        #print('{0:<30}\t{1:<30}\t{2:^3}\t{3:^3}\t{4:^4}\t{5:^3}\t{6:^3}\t{7:^5}'.format('BATSMEN', '', 'R', 'B', 'M', '4s', '6s', 'SR'))
        op_file.write('{0:<30}\t{1:<30}\t{2:^3}\t{3:^3}\t{4:^4}\t{5:^3}\t{6:^3}\t{7:^5}\n'.format('BATSMEN', '', 'R', 'B', 'M', '4s', '6s', 'SR'))
        #print(self.team1.batsmen.keys())
        for bt in self.team1.batsmen.keys():
            if self.team1.batsmen[bt].dismissalType == 'did not bat':
                continue
            #print('{0:<30}\t{1:<30}\t{2:^3}\t{3:^3}\t{4:^4}\t{5:^3}\t{6:^3}\t{7:.2f}'.format(
            #                                self.team1.batsmen[bt].bt_name, self.team1.batsmen[bt].dismissalType, self.team1.batsmen[bt].runs,
            #                                self.team1.batsmen[bt].balls, self.team1.batsmen[bt].time, self.team1.batsmen[bt].fours,
            #                                self.team1.batsmen[bt].sixes, self.team1.batsmen[bt].strike_rate))
            op_file.write('{0:<30}\t{1:<30}\t{2:^3}\t{3:^3}\t{4:^4}\t{5:^3}\t{6:^3}\t{7:.2f}\n'.format(
                                            self.team1.batsmen[bt].bt_name, self.team1.batsmen[bt].dismissalType, self.team1.batsmen[bt].runs,
                                            self.team1.batsmen[bt].balls, self.team1.batsmen[bt].time, self.team1.batsmen[bt].fours,
                                            self.team1.batsmen[bt].sixes, self.team1.batsmen[bt].strike_rate))

        wide_runs = self.team1.extra_runs - self.team1.leg_bye_runs - self.team1.bye_runs - self.team1.no_ball_runs
        #print('Extras {0:<4} ( lb {1:>2}, b {2:>2}, nb {3:>2}, w {4:>2} )'.format(self.team1.extra_runs, self.team1.leg_bye_runs, self.team1.bye_runs, self.team1.no_ball_runs, wide_runs))
        op_file.write('Extras {0:<4} ( lb {1:>2}, b {2:>2}, nb {3:>2}, w {4:>2} )\n'.format(self.team1.extra_runs, self.team1.leg_bye_runs, self.team1.bye_runs, self.team1.no_ball_runs, wide_runs))
        #print('TOTAL {0:4}'.format(self.team1.total_runs))
        op_file.write('TOTAL {0:4}/{1:<2}\n'.format(self.team1.total_runs, self.team1.wickets))

        #print('Did not bat : ')
        op_file.write('Did not bat: ')
        did_not_bat_str = ''
        for i in range(len(team1_did_not_bat)):
            did_not_bat_str += (team1_did_not_bat[i] + ', ')
        #print(did_not_bat_str)
        op_file.write(did_not_bat_str + '\n')

        #print('Fall of wickets: ')
        op_file.write('Fall of wickets: ')
        fall_str = ''
        for i in sorted(team1_fall_of_wicket.keys()):
            fall_str += str(team1_fall_of_wicket[i][1])
            fall_str += ('-')
            fall_str += str(team1_fall_of_wicket[i][2])
            fall_str += ' ({0}, {1}), '.format(team1_fall_of_wicket[i][0], team1_fall_of_wicket[i][3])
            
        #print(fall_str)
        op_file.write(fall_str + '\n')

        #print('\nBowling Scorecard')
        op_file.write('\nBowling Scorecard\n')
        #print('{0:<30}  {1:^4}  {2:^3}  {3:^3}  {4:^4}  {5:^6}  {6:^3}  {7:^5}  {8:^3}  {9:^3}  {10:^3}'.format('Bowler', 'O', 'M', 'R', 'W', 'Econ', '0s', '4s', '6s', 'WD', 'NB'))
        op_file.write('{0:<30}  {1:^4}  {2:^3}  {3:^3}  {4:^4}  {5:^6}  {6:^3}  {7:^5}  {8:^3}  {9:^3}  {10:^3}\n'.format('Bowler', 'O', 'M', 'R', 'W', 'Econ', '0s', '4s', '6s', 'WD', 'NB'))
        for bw in self.team2.bowlers.keys():
            #print('{0:<30}  {1:^4}  {2:^3}  {3:^3}  {4:^4}  {5:^6}  {6:^3}  {7:^5}  {8:^3}  {9:^3}  {10:^3}'.format(
            #                                self.team2.bowlers[bw].bw_name, self.team2.bowlers[bw].complete_overs + (self.team2.bowlers[bw].incomplete_over_balls / 10),
            #                                self.team2.bowlers[bw].maiden_overs, self.team2.bowlers[bw].runs, self.team2.bowlers[bw].wickets,
            #                                '{0:.2f}'.format(self.team2.bowlers[bw].economy), self.team2.bowlers[bw].zero_balls, self.team2.bowlers[bw].fours,
            #                                self.team2.bowlers[bw].sixes, self.team2.bowlers[bw].wide_balls, self.team2.bowlers[bw].no_balls))
            op_file.write('{0:<30}  {1:^4}  {2:^3}  {3:^3}  {4:^4}  {5:^6}  {6:^3}  {7:^5}  {8:^3}  {9:^3}  {10:^3}\n'.format(
                                            self.team2.bowlers[bw].bw_name, self.team2.bowlers[bw].complete_overs + (self.team2.bowlers[bw].incomplete_over_balls / 10),
                                            self.team2.bowlers[bw].maiden_overs, self.team2.bowlers[bw].runs, self.team2.bowlers[bw].wickets,
                                            '{0:.2f}'.format(self.team2.bowlers[bw].economy), self.team2.bowlers[bw].zero_balls, self.team2.bowlers[bw].fours,
                                            self.team2.bowlers[bw].sixes, self.team2.bowlers[bw].wide_balls, self.team2.bowlers[bw].no_balls))




        #print('Printing')

        #print(self.team1.batsmen.keys())
        #print(self.team1.bowlers.keys())
        #print(self.team2.batsmen.keys())
        #print(self.team2.bowlers.keys())


exitFlag = 0

class myThread (threading.Thread):
   def __init__(self, threadID, name, delay):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = name
      self.counter = delay
   def run(self):
      cons = KafkaConsumer(self.name, bootstrap_servers = ['localhost:9092'])
      with open('194161007-'+self.name+'-commentary-consumed.txt', 'w') as op_file:
         for msg in cons:
            m = msg.value.decode('utf-8')
            if m == 'File End!!!':
               break
            print(self.name, m)
            if m[-1] == '\n':
               op_file.write(msg.value.decode('utf-8'))
            else:
               op_file.write(msg.value.decode('utf-8') + '\n')
            
            try:
               m1 = MatchScorecard(self.name, 't1', 't2')
               match_abandoned_flag = 1111
               match_abandoned_flag = m1.getStats()
               if match_abandoned_flag != -1111:
                  m1.updatePlayerNames()
                  m1.printStats()
                  match_abandoned_flag = 1111
               else:
                  print('ABANDONED')
            except:
               pass


# Create new threads
thread_names = {
                1:'4143', 2:'4144', 3:'4145',4:'4146',5:'4147',6:'4148',
                7:'4149',8:'4150',9:'4151',10:'4152',11:'4152a', 12:'4153',
                13:'4154',14:'4155',15:'4156',16:'4156a', 17:'4157', 18:'4157a',
                19:'4158',20:'4159', 21:'4160', 22:'4161', 23:'4162', 24:'4163',
                25:'4165', 26:'4166', 27:'4168', 28:'4169', 29:'4170', 30:'4171',
                31:'4172', 32:'4173', 33:'4174', 34:'4175', 35:'4176', 36:'4177',
                37:'4178', 38:'4179', 39:'4180', 40:'4182', 41:'4183', 42:'4184',
                43:'4186', 44:'4187', 45:'4188', 46:'4190', 47:'4191', 48:'4192'
                }

threads = []
for t_n in thread_names.keys():
   thrd = myThread(t_n, thread_names[t_n],t_n)
   threads.append(thrd)

# Start new Threads
for t in threads:
   t.start()

for t in threads:
   t.join()

print ("Exiting Main Thread")