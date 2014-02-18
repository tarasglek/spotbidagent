import boto.ec2
import pickle
import logging

log = logging.getLogger(__name__)


def get_spot_price_history(region):
        r = boto.ec2.get_region(region)
        # TODO: use secrets
        ec2 = r.connect()
        return ec2.get_spot_price_history()


def get_current_spot_prices(region):
    all_prices = get_spot_price_history(region)
    # make sure to sort them by the timestamp, so we don't process the same
    # entry twice
    all_prices = sorted(all_prices, key=lambda x: x.timestamp, reverse=True)
    current_prices = {}
    for price in all_prices:
        az = price.availability_zone
        instance_type = price.instance_type
        if not current_prices.get(instance_type):
            current_prices[instance_type] = {}
        if not current_prices[instance_type].get(az):
            current_prices[instance_type][az] = price.price
    return current_prices


def download_pricelist(region):
    with open("price.%s.pickle" % (region), "w") as out:
        history = get_spot_price_history(region)
        pickle.dump(history, out)
    return history


def download():
    map(download_pricelist, ['eu-west-1', 'us-west-2', 'us-east-1',
                             'us-west-1'])


def get_pricelist_disk(region):
    history = pickle.load(open("price.%s.pickle" % (region)))
    return history


class Spot:
    def __init__(self, instance_type, availability_zone, current_price,
                 bid_price, performance_constant):
        self.instance_type = instance_type
        self.availability_zone = availability_zone
        self.current_price = current_price
        self.bid_price = bid_price
        self.performance_constant = performance_constant

    def __repr__(self):
        return "<%s (%s) %g (value: %g) < %g" % (
            self.instance_type, self.availability_zone,
            self.current_price, self.value(), self.bid_price)

    def __str__(self):
        return self.__repr__()

    def value(self):
        return self.current_price / self.performance_constant

    def __lt__(self, other):
        return self.value() < other.value()


def decide(rules, regions):
    choices = []
    prices = {}
    for region in regions:
        prices.update(get_current_spot_prices(region))
    for rule in rules:
        instance_type, bid_price, perf_const = rule
        for az, price in prices.get(instance_type, {}).iteritems():
            if price > bid_price:
                log.debug("%s (in %s) too expensive", price, az)
            else:
                choices.append(Spot(instance_type, az, price,
                                    bid_price, perf_const))
    choices.sort()
    return choices

if __name__ == "__main__":
    ret = decide(
        [["c3.xlarge", 0.250, 1],
         ["m3.xlarge", 0.250, 1.1],
         ["c3.2xlarge", 0.250, 1.2]],
        ['us-west-2', 'us-east-1'])
    print "\n".join(map(str, ret))
