import group_prod_rating
import group_airline_rating
import capacity_rating_cap
import capacity_rating_freq
import capacity_rating_blocktime
import market_rating_mktshare
import Restriction_Final
import Agility
import Agentsf1_Final
import group_market_rating_No_of_competitors
import group_market_rating_Growth_of_market3
import group_market_rating_Size_of_Market
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def main():
    try:
        print "Called Product Rating"
        group_prod_rating.run()
    except Exception, e:
        print e
        pass
    try:
        print "Called Airline Rating"
        group_airline_rating.run()
    except Exception, e:
        print e
        pass
    try:
        print "Called Capacity Rating"
        capacity_rating_cap.run()
    except Exception, e:
        print e
        pass
    try:
        print "Called Capacity_Freq Rating"
        capacity_rating_freq.run()
    except Exception, e:
        print e
        pass
    try:
        print "Called Capacity_Blocktime Rating"
        capacity_rating_blocktime.run()
    except Exception, e:
        print e
        pass
    try:
        print "Called Market Rating"
        market_rating_mktshare.run()
    except Exception, e:
        print e
        pass
    try:
        print "Called Fares Rating"
        Restriction_Final.run()
    except Exception, e:
        print e
        pass
    try:
        print "Called Fares1 Rating"
        Agility.run()
    except Exception, e:
        print e
        pass
    try:
        print "Called Distributors Rating"
        Agentsf1_Final.run()
    except Exception, e:
        print e
        pass
    try:
        print "Called Market Rating2"
        group_market_rating_No_of_competitors.run()
    except Exception, e:
        print e
        pass
    try:
        print "Called Market Rating3"
        group_market_rating_Growth_of_market3.run()
    except Exception, e:
        print e
        pass
    try:
        print "Called Market Rating4"
        group_market_rating_Size_of_Market.run()
    except Exception, e:
        print e
        pass


if __name__ == '__main__':
    main()

