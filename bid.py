from boto.ec2 import connect_to_region
import logging

log = logging.getLogger(__name__)


def get_current_spot_prices(connection):
    all_prices = connection.get_spot_price_history()
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


class Spot:
    def __init__(self, instance_type, availability_zone, current_price,
                 bid_price, performance_constant):
        self.instance_type = instance_type
        self.availability_zone = availability_zone
        self.current_price = current_price
        self.bid_price = bid_price
        self.performance_constant = performance_constant

    def __repr__(self):
        return "%s (%s) %g (value: %g) < %g" % (
            self.instance_type, self.availability_zone,
            self.current_price, self.value(), self.bid_price)

    def __str__(self):
        return self.__repr__()

    def value(self):
        return self.current_price / self.performance_constant

    def __cmp__(self, other):
        return cmp(self.value(), other.value())


def decide(connections, rules):
    choices = []
    prices = {}
    for connection in connections:
        prices.update(get_current_spot_prices(connection))
    for rule in rules:
        instance_type = rule["instance_type"]
        bid_price = rule["bid_price"]
        performance_constant = rule["performance_constant"]
        for az, price in prices.get(instance_type, {}).iteritems():
            if price > bid_price:
                log.debug("%s (in %s) too expensive", price, az)
            else:
                choices.append(Spot(instance_type, az, price,
                                    bid_price, performance_constant))
    # sort by self.value()
    choices.sort()
    return choices

if __name__ == "__main__":
    connections = []
    for region in ['us-west-2', 'us-east-1']:
        # FIXME: user secrets
        connections.append(connect_to_region(region))
    rules = [
        {
            "instance_type": "c3.xlarge",
            "performance_constant": 1,
            "bid_price": 0.25
        },
        {
            "instance_type": "m3.xlarge",
            "performance_constant": 1.1,
            "bid_price": 0.25
        },
        {
            "instance_type": "c3.2xlarge",
            "performance_constant": 1.2,
            "bid_price": 0.25
        },
    ]
    ret = decide(connections, rules)
    print "\n".join(map(str, ret))
