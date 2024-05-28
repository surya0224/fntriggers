import math
import smtplib
import urllib2
from datetime import date
from datetime import timedelta
from email.mime.text import MIMEText
from time import localtime, strftime

import numpy
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
obj = SentimentIntensityAnalyzer()
from jupiter_AI import client, JUPITER_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure
db = client[JUPITER_DB]

airlines = [
    "AIR ASIA ",
    "AIR CANADA ",
    "ALASKA AIRLINES ",
    "British_Airways",
    "EMIRATES",
    "INDIGO ",
    "JET AIRWAYS ",
    "KLM",
    "KOREAN ",
    "QATAR",
    "SAUDIA",
    "SINGAPORE AIRLINES ",
    "SOUTH AFRICAN AIRLINES ",
    "VIRGIN AMRERICA "
]
cl = [
    "JUP_DB_Tweeter_AIR ASIA ",
    "JUP_DB_Tweeter_AIR CANADA ",
    "JUP_DB_Tweeter_ALASKA AIRLINES ",
    "JUP_DB_Tweeter_British_Airways",
    "JUP_DB_Tweeter_EMIRATES",
    "JUP_DB_Tweeter_INDIGO ",
    "JUP_DB_Tweeter_JET AIRWAAYS ",
    "JUP_DB_Tweeter_KLM",
    "JUP_DB_Tweeter_KOREAN ",
    "JUP_DB_Tweeter_QATAR",
    "JUP_DB_Tweeter_SAUDIA",
    "JUP_DB_Tweeter_SINGAPORE AIRLINES ",
    "JUP_DB_Tweeter_SOUTH AFRICAN AIRLINES ",
    "JUP_DB_Tweeter_VIRGIN AMRERICA "
]


@measure(JUPITER_LOGGER)
def for_an_airline(collection, date1, date2=None):
    """

    :param collection: airline collection in social db from which the data is being acquired
    :param date1: start date of the analysis
    :param date2: end date of the analysis
    :return: dictionery containing content score,applause score, amplification and no. of tweets
    """
    cursor = db[collection].find({"Date": {"$gte": date1, "$lt": date2}})
    print cursor.count()
    # content score of tweets list
    c_score = []
    # favourite_count of tweet list
    applause = []
    # retweet_count of tweet list
    amplification = []
    if cursor.count() != 0:
        k = 0
        for j in cursor:
            g = 1
            while True:
                try:
                    # translate tweet in english
                    r = translate(j['Tweet'].encode('utf8'), "en")
                    break
                except Exception as e:
                    continue
            vs = obj.polarity_scores(r)
            # give content score through sentiment analyser
            vs = vs['compound']
            # check for polarity of tweet
            if vs < 0:
                g = 0
            vs = math.pow(vs, 2)
            vs = math.sqrt((15 * vs) / (1 - vs))
            if g == 0:
                vs = vs * -1
            c_score.append(vs)
            applause.append(j['Favorite'])
            amplification.append(j['Retweet'])
            k += 1
        # average content score
        c_avg = numpy.mean(c_score)
        app_avg = numpy.mean(applause)
        amp_avg = numpy.mean(amplification)
        #   normalized content score
        c_avg = normalize(c_avg)
        #   storing result in dictionary p
        dd = {"Content Score": c_avg,
              "Applause Score": app_avg,
              "Amplification": amp_avg,
              "No of Tweets": len(c_score)}
        return dd
    else:
        return None


# Translate Function, translates tweets to english


@measure(JUPITER_LOGGER)
def translate(to_translate, to_language="auto", language="auto"):
    """
    Return the translation using google translate
    you must shortcut the language you define (French = fr, English = en, Spanish = es, etc...)
    if you don't define anything it will detect it or use english by default
    Example:
    print(translate("salut tu vas bien?", "en"))
    hello you alright?"""
    agents = {
        'User-Agent': "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30)"}
    before_trans = 'class="t0">'
    link = "http://translate.google.com/m?hl=%s&sl=%s&q=%s" % (to_language, language, to_translate.replace(" ", "+"))
    request = urllib2.Request(link, headers=agents)
    page = urllib2.urlopen(request).read()
    result = page[page.find(before_trans) + len(before_trans):]
    result = result.split("<")[0]
    return result


# content score normalization


@measure(JUPITER_LOGGER)
def normalize(score, alpha=15):
    # normalize the score to be between -1 and 1 using an alpha that approximates the max expected value
    normScore = score / math.sqrt(((score * score) + alpha))
    return normScore


@measure(JUPITER_LOGGER)
def main():
    # list of twitter collection names in mongodb database
    startdate = date.today()
    enddate = startdate + timedelta(days=1)
    startdate = str(startdate)
    print startdate
    enddate = str(enddate)
    print enddate
    try:
        p = dict()
        p['date'] = startdate
        for i in range(len(airlines)):
            p[airlines[i]] = for_an_airline(cl[i], startdate, enddate)
        db.JUP_DB_Airline_Twitter_Analysis.insert_one(p)
    except Exception as e:
        import traceback
        from time import strftime, localtime
        from jupiter_AI.common.mail_error_msg import send_simple_message
        from jupiter_AI import NOTIFICATION_EMAIL

        p = ''.join(['ERROR : ',
                     traceback.format_exc(),
                     ' \nTIME : ',
                     strftime("%a, %d %b %Y %H:%M:%S ", localtime()),
                     'IST'])
        send_simple_message(to=NOTIFICATION_EMAIL, subject='Twitter Analysis Codes', body=p)

if __name__ == '__main__':
    pass
    # main()
