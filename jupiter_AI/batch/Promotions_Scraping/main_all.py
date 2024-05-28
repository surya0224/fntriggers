import cathay_pacific_proms
import Emirates_Promotions
import Etihad_Airlines_Promotions
import FlyDubai_Promotions
import malaysianairlines_proms
import Middle_East_Airlines_Promotions
import omanair_proms
import Qantas_Airlines_Promotions
import Royal_Brunei_Airlines_Promotions
import Singapore_Airlines_Promotions
import ethiopian_proms_new
#import airarabia_proms
import EK_new
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def main():
    try:
        print "Called EY"
        Etihad_Airlines_Promotions.run()
    except Exception, e:
        print e
        pass
    try:
        print "Called EK"
        EK_new.run()
    except Exception, e:
        print e
        pass


if __name__ == '__main__':
    main()
