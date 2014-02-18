import boto.ec2
import simplejson as json
import sys
import pickle

def download_pricelist(region):
    with open("price.%s.pickle" % (region), "w") as out:
        r = boto.ec2.get_region(region)
        ec2 = r.connect()
        history = ec2.get_spot_price_history(product_description="Linux/UNIX");
        pickle.dump(history, out);
    return history

def download():
    map(download_pricelist, ['eu-west-1', 'us-west-2','us-east-1', 'us-west-1'])

def get_pricelist_disk(region):
    history = pickle.load(open("price.%s.pickle" % (region)))
    return history

class Spot:
    def __init__(self, spotEntry, max_price, performance_constant):
        self.spotEntry = spotEntry
        self.max_price = max_price
        self.performance_constant = performance_constant
    def __repr__(self):
        return "%s(%s) %g(%g) < %g" % (self.spotEntry.instance_type, self.spotEntry.availability_zone,
                                   self.spotEntry.price, self.value(), self.max_price)
    def __str__(self):
        return self.__repr__()

    def value(self):
        return self.spotEntry.price / self.performance_constant

    def __lt__(self, other):
        return self.value() < other.value()

def decide(rules, regions):
    def sort_newest2oldest(ls):
        def cmp(a, b):
            if b.timestamp == a.timestamp:
                return 0
            elif b.timestamp > a.timestamp:
                return 1
            else:
                return -1
        return sorted(ls, cmp)
    choices = []
    regions = map(sort_newest2oldest, regions)
    for r in rules:
        if type(r[2]) == str:
            continue
        spot_azs = {}
        for history in regions:
            for h in history:
                if h.instance_type != r[0]:
                    continue
                if not h.availability_zone in spot_azs:
                    spot_azs[h.availability_zone] = h
           
            if len(spot_azs) == 0:
                print "No spot info for %s" % r[0]
                continue

        for az, spot in spot_azs.iteritems():
            if spot.price < r[1]:
                s = Spot(spot, r[1], r[2])
                choices.append(s)
            else:
                #print "%s(in %s) too expensive as of %s" % (spot, spot.availability_zone, spot.timestamp)
                pass
    return sorted(choices)

def main():
    regions = ['us-west-2','us-east-1', 'us-west-1', 'eu-west-1']
    if len(sys.argv) > 1:
        print "Using cached data"
        regions = map(get_pricelist_disk, regions)
    else:
        print "Querying AWS"
        regions = map(download_pricelist, regions)
        
    ret = decide([
        ["m3.large", 0.150, 0.4],
        ["m2.2xlarge", 0.250, 0.9],
        ["c3.xlarge", 0.250, 1],
        ["m3.xlarge", 0.250, 1.1],
        ["c1.xlarge", 0.250, 1.2],
        ["c3.2xlarge", 0.250, 1.3],
        ["g2.2xlarge ", 0.250, 1.3],
        ["c3.xlarge", 0.300, "ondemand"]
    ], regions)
    print "\n".join(map(str, ret))
    
if __name__ == "__main__":
    main()
