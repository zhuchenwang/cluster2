'use strict';

var request = require('request');
var should = require('should');
var fork = require('child_process').fork;
var when = require('when');
var parallel = require('when/parallel');
var util = require('util');

describe('Cache Performance Test', function () {

    var childProc;
    var key = 'cache-test-key';
    var value = 'cache-test-value';
    var writeTask = function () {
        var deferred = when.defer();
        request.get(util.format('http://127.0.0.1:9090/set?key=%s&value=%s', key, value), function (err, res, body) {
            if (!err && res.statusCode === 200 && body === value) {
                //console.log(process.pid + ': set ' + key + '=' + value);
                deferred.resolve(body);
            }else {
                deferred.reject(err || 'cache set error');
            }
        });
        return deferred.promise;
    };
    var readTask = function () {
        var deferred = when.defer();
        request.get(util.format('http://127.0.0.1:9090/get?key=%s', key), function (err, res, body) {
            if (!err && res.statusCode === 200 && (body === value || body === 'cache-test')) {
                //console.log(process.pid + ': get ' + key);
                deferred.resolve(body);
            }else {
                deferred.reject(err || 'cache get error');
            }
        });
        return deferred.promise;
    };

    beforeEach(function (done) {
        var token = 't-' + Date.now();
        childProc = fork(require.resolve('./lib/cluster-cache-runtime.js'), ['--token=' + token]);
        childProc.on('message', function (msg) {
            if(msg.ready) {
                return done();
            }
            if (msg.err) {
                return done(err);
            }
        });
    });

    afterEach(function (done) {
        childProc.kill('SIGTERM');
        done();
    });

    /*describe('# concurrent set', function () {
        var tasks = [];
        for (var i = 0; i < 200; i++) {
            tasks.push(readTask);
        }
        it('should pass', function (done) {
            parallel(tasks).then(function (values) {
                done();
            }).otherwise(function (err) {
                done(err);
            });
        });
    });*/
    describe('# 90% read, %10 write', function () {
        var tasks = [];
        for (var i=0; i<200; i++) {
            if (i % 10 === 0) {
                tasks.push(writeTask);
            }else {
                tasks.push(readTask);
            }
        }
        it('Should resolve all the promises', function (done) {
            var startTime = Date.now();
            parallel(tasks).then(function (values) {
                var duration = Date.now() - startTime;
                console.log(duration / 1000);
                done();
            }).otherwise(function (err) {
                done(err);
            });
        });
    });

    describe('# 50% read, 50% write', function () {
        var tasks = [];
        for (var i=0; i<200; i++) {
            if (i % 10 < 5) {
                tasks.push(writeTask);
            }else {
                tasks.push(readTask);
            }
        }
        it('Should resolve all the promises', function (done) {
            var startTime = Date.now();
            parallel(tasks).then(function (values) {
                var duration = Date.now() - startTime;
                console.log(duration / 1000);
                done();
            }).otherwise(function (err) {
                done(err);
            });
        });
    });

    describe('# 10% read, 90% write', function () {
        var tasks = [];
        for (var i=0; i<200; i++) {
            if (i % 10 < 9) {
                tasks.push(writeTask);
            }else {
                tasks.push(readTask);
            }
        }
        it('Should resolve all the promises', function (done) {
            var startTime = Date.now();
            parallel(tasks).then(function (values) {
                var duration = Date.now() - startTime;
                console.log(duration / 1000);
                done();
            }).otherwise(function (err) {
                done(err);
            });
        });
    });
});
