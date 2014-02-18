import boto.ec2
import pickle


def download_pricelist(region):
    with open("price.%s.pickle" % (region), "w") as out:
        r = boto.ec2.get_region(region)
        ec2 = r.connect()
        history = ec2.get_spot_price_history()
        pickle.dump(history, out)
    return history


def download():
    map(download_pricelist, ['eu-west-1', 'us-west-2', 'us-east-1',
                             'us-west-1'])


def get_pricelist_disk(region):
    history = pickle.load(open("price.%s.pickle" % (region)))
    return history


class Spot:
    def __init__(self, spotEntry, max_price, performance_constant):
        self.spotEntry = spotEntry
        self.max_price = max_price
        self.performance_constant = performance_constant

    def __repr__(self):
        return "%s(%s) %g(%g) < %g" % (
            self.spotEntry.instance_type, self.spotEntry.availability_zone,
            self.spotEntry.price, self.value(), self.max_price)

    def __str__(self):
        return self.__repr__()

    def value(self):
        return self.spotEntry.price / self.performance_constant

    def __lt__(self, other):
        return self.value() < other.value()


def decide(rules, regions):
    choices = []
    regions = map(get_pricelist_disk, regions)
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
                    continue
                s = spot_azs[h.availability_zone]
                if s.timestamp <= h.timestamp:
                    spot_azs[h.availability_zone] = h
                    print "this never happens because boto sorts these"
            if len(spot_azs) == 0:
                print "No spot info for %s" % r[0]
                continue

            for az, spot in spot_azs.iteritems():
                if spot.price < r[1]:
                    choices.append(Spot(spot, r[1], r[2]))
                else:
                    print "%s(in %s) too expensive as of %s" % (
                        spot, spot.availability_zone, spot.timestamp)
            print spot_azs.keys()
    print "\n".join(map(str, sorted(choices)))
            #print history

if __name__ == "__main__":
    ret = decide(
        [["c3.xlarge", 0.250, 1],
         ["m3.xlarge", 0.250, 1.1],
         ["m3.large", 0.150, 0.6],
         ["c3.2xlarge", 0.250, 1.2],
         ["c3.xlarge", 0.300, "ondemand"]],
        ['us-west-2', 'us-east-1', 'us-west-1', 'eu-west-1'])
