'use strict';

var util = require('util');
var Worker = require('./worker.js').Worker;
var assert = require('assert');
var when = require('when');
var timeout = require('when/timeout');

var CacheMgrWorker = exports.CacheMgrWorker = function (proc, options) {
	
	var _this = this.initialize(proc, options);
    _this.logger = proc.getLogger(__filename);
    return _this;
};

util.inherits(CacheMgrWorker, Worker);

CacheMgrWorker.prototype.listen = function () {
    var _this = this;
	if (!_this.process.env.CACHE_MANAGER) {
		// this is a normal worker
		// call parent's method
		return CacheMgrWorker.super_.prototype.listen.apply(_this);
	}
	// real cache manager worker
	var	app = _this.app,
		port = _this.port,
		createServer = _this.createServer,
		configureApp = _this.configureApp,
		warmUp = _this.warmUp,
		debug = _this.debug,
		wait = _this.timeout;

	assert.ok(createServer);
	assert.ok(app);
	assert.ok(port);
	assert.ok(configureApp);
	assert.ok(warmUp);

	var tillListen = when.defer();
	var axon = require('axon');
	//_this.logger.info('[worker] %d is the cache manager', _this.pid);
    var run = function () {
        var server = createServer(app, port);
        
        server.once('listening', function (conn) {
        	_this.logger.debug('[cache-manager] %d started listening', _this.pid);
			_this.emitter.to(['master', 'self']).emit(util.format('worker-%d-listening', _this.pid), _this.pid);
					
			//connection monitoring, including live/total connections and idle connections
			server.on('connection', function(conn){
				_this.whenConnected(conn);
			});
			
            when(warmUp()).ensure(function (warmedUp) {
            	//_this.logger.info('[cache-manager] %d warmed up', _this.pid);
            	_this.emitter.to(['master', 'self']).emit(util.format('worker-%d-warmup', _this.pid), _this.pid);
            	tillListen.resolve({
                	'master': null,
                	'worker': _this,
                	'port': port,
           			'app': app,
					'server': server
				});
            });
		});
	};
	
	if(!debug){ //normal
		run();
	}
	else{ //debug fresh process, waiting for 'run' command
		_this.emitter.once('run', run);
	}

	return (wait > 0 ? timeout(_this.timeout, tillListen.promise) : tillListen.promise);
};

CacheMgrWorker.prototype.whenStop = function () {
    //process.repSock.close();
    //process.pubSock.close();
    CacheMgrWorker.super_.prototype.whenStop.apply(this);
};
