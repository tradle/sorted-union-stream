var util = require('util');
var Readable = require('stream').Readable;

var stream2 = function(stream) {
	if (stream._readableState) return stream;
	return new Readable({objectMode:true, highWaterMark:16}).wrap(stream);
};

var destroy = function(stream) {
	if (stream.readable && stream.destroy) stream.destroy();
};

var defaultKey = function(valA, valB) {
	if (valA === valB) return 0;
	return valA < valB ? -1 : 1;
};

var reader = function(self, stream, toKey) {
	var ended = false;
	var data = null;
	var key = null;
	var fn;

	var consume = function() {
		data = null;
		key = null;
	};

	var onresult = function() {
		if (!fn) return;
		var tmp = fn;
		fn = undefined;
		tmp(data, key, consume);
	};

	var update = function() {
		if (!fn) return;
		data = stream.read();
		if (data === null && !ended) return;
		key = toKey(data);
		onresult();
	};

	var onend = function() {
		ended = true;
		onresult();
	};

	stream.on('readable', update);

	stream.on('error', function(err) {
		self.emit('error', err);
		self.destroy();
	});

	stream.on('close', function() {
		if (stream._readableState.ended) return;
		onend();
	});

	stream.on('end', onend);

	return function(callback) {
		if (data) return callback(data, consume);
		if (ended) return callback(null, consume);
		fn = callback;
		update();
	};
};

var Union = function(a, b, toKey) {
	if (!(this instanceof Union)) return new Union(a, b, toKey);
	Readable.call(this, {objectMode:true, highWaterMark:16});
	if (!toKey) toKey = defaultKey;

	this._destroyed = false;
	this._a = a;
	this._b = b;

	this._readA = reader(this, a, toKey);
	this._readB = reader(this, b, toKey);
};

util.inherits(Union, Readable);

Union.prototype._read = function() {
	var self = this;
	this._readA(function(valA, keyA, consumeA) {
		self._readB(function(valB, keyB, consumeB) {
			if (!valA && !valB) return self.push(null);

			if (!valA) {
				consumeB();
				return self.push(valB);
			}
			if (!valB) {
				consumeA();
				return self.push(valA);
			}

			if (keyA === keyB) return consumeA();

			if (keyA < keyB) {
				consumeA();
				return self.push(valA);
			}

			consumeB();
			self.push(valB);
		});
	});
};

Union.prototype.destroy = function() {
	if (this._destroyed) return;
	this._destroyed = true;
	destroy(this._a);
	destroy(this._b);
	this.push(null);
};

module.exports = Union;