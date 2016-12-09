const levelup = require('levelup');

class SolidArray {
    constructor(config) {
        this.config = config;
        if (!config) {
            throw new Error('Configuration is required');
        }
        this.db = levelup(config.dbfile);
    }

    push(value, key, cb) {
        if (!value) {
            throw new Error('value is required');
        }
        if (!key) {
            throw new Error('key is required');
        }
        if (!cb) {
            throw new Error('A callback function is required');
        }
        this.db.put(value, key, function (err) {
            if (err) throw new Error(err);
            cb();
        });
    }

    //TODO: Run this in parallel
    pushArray(valueList, key, cb) {
        for (let value of valueList) {
            this.db.put(value, key, function (err) {
                if (err) throw new Error(err);
            });
        }
        cb();
    }

    pullN(number, biz, cb) {
        let counters = {};
        let res = {};
        let that = this;
        this.db.createReadStream()
            .on('data', function (data) {
                if (!counters[data.value]) {
                    counters[data.value] = 0;
                    res[data.value] = [];
                }
                if (++counters[data.value] <= number && data.value === biz) {
                    that.db.del(data.key, function (err) {
                        res[data.value].push(data.key);
                        if (err) {
                            throw new Error(err);
                        }
                    });
                }
            })
            .on('error', function (err) {
                throw new Error(err);
            })
            .on('close', function () {
                return cb(res[biz]);
            });
    }

    pullAll(biz, cb) {
        return this.pullN(Infinity, biz, cb)
    }

    pull(biz, cb) {
        this.pullN(1, biz, (res) => {
            cb(res[0])
        });
    }
}

module.exports = SolidArray;