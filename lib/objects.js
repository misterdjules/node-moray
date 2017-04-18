/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * lib/objects.js: object-related client API functions.  These functions are
 * invoked by same-named methods in lib/client.js to do the bulk of the work
 * associated with making RPC requests.  The arguments and semantics of these
 * functions are documented in the Moray API.
 */

var EventEmitter = require('events').EventEmitter;

var assert = require('assert-plus');
var jsprim = require('jsprim');
var libuuid = require('libuuid');
var VError = require('verror');

var rpc = require('./rpc');


///--- API

function putObject(rpcctx, bucket, key, value, options, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.string(key, 'key');
    assert.object(value, 'value');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    opts = makeOptions(options, value);
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'putObject',
        'rpcargs': [ bucket, key, value, opts ]
    }, function (err, data) {
        if (!err && data.length > 1) {
            err = new VError('expected at most 1 data message, found %d',
                data.length);
        }

        if (err) {
            callback(err);
        } else {
            callback(null, data.length === 0 ? {} : data[0]);
        }
    });
}

function getObject(rpcctx, bucket, key, options, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.string(key, 'key');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    opts = makeOptions(options);
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'getObject',
        'rpcargs': [ bucket, key, opts ]
    }, function (err, data) {
        if (!err && data.length != 1) {
            err = new VError('expected exactly 1 data message, found %d',
                data.length);
        }

        if (err) {
            callback(err);
        } else {
            callback(null, data[0]);
        }
    });
}

function deleteObject(rpcctx, bucket, key, options, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.string(key, 'key');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    opts = makeOptions(options);
    log = rpc.childLogger(rpcctx, opts);

    /*
     * electric-moray sends trailing null values with this response.  These are
     * not normally allowed unless we specify ignoreNullValues.
     */
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'ignoreNullValues': true,
        'rpcmethod': 'delObject',
        'rpcargs': [ bucket, key, opts ]
    }, function (err, data) {
        /*
         * The server provides data in a response, but historically this client
         * ignores it.
         */
        callback(err);
    });
}

/*
 * Returns an array of strings representing the name of options that should have
 * been explicitly marked as handled by a moray server, but were not. Returns an
 * empty array in case this set is empty.
 *
 * @param {Object} options - represents the options passed to a findObjects
 * request.
 *
 * @param {Object} handledOptions - represents the options that were actually
 * acknowledged as handled by the moray server that served this findObjects
 * request.
 *
 * @param {Object} optionsSpec - stores functions that determine when the value
 * of an option passed to a findObjects request means that the use of that
 * option needs to be handled by the moray server serving that request. For
 * instance, when passing: `requireIndexes: false` to a findObjects request,
 * there's no need to require the server to acknowledge that it can handle this
 * option.
 */
function getUnhandledOptions(options, handledOptions, optionsSpec) {
    assert.object(options, 'options');
    assert.object(handledOptions, 'handledOptions');
    assert.object(optionsSpec, 'optionsSpec');

    var optionName;
    var optionValue;
    var testNeedHandlingFn;
    var unhandledOptions = [];

    for (optionName in options) {
        if (!Object.hasOwnProperty.call(optionsSpec, optionName)) {
            continue;
        }

        optionValue = options[optionName];

        testNeedHandlingFn = optionsSpec[optionName].testNeedHandling;
        assert.func(testNeedHandlingFn, 'testNeedHandlingFn');

        if (testNeedHandlingFn(optionValue) &&
            handledOptions[optionName] === false) {
            unhandledOptions.push(optionName);
        }
    }

    return (unhandledOptions);
}

/*
 * Emits an UnhandledOptionsError error event on the event emitter "res".
 *
 * @param {Object} res - the response object on which to emit the error.
 *
 * @param {Array} unhandledOptions - an array of strings that represents the
 * name of options that should have been marked as explicitly handled by the
 * moray server serving a findObjects request.
 */
function emitUnhandledOptionsError(res, unhandledOptions) {
    assert.object(res, 'res');
    assert.object(unhandledOptions, 'unhandledOptions');

    var err = new Error('Unhandled options: ' + unhandledOptions.join(', '));
    err.name = 'UnhandledOptionsError';
    err.unhandledOptions = unhandledOptions;

    res.emit('error', err);
}

function findObjects(rpcctx, bucket, filter, options) {
    var opts, log, req, res;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.string(filter, 'filter');
    assert.object(options, 'options');
    assert.optionalBool(options.requireIndexes, 'options.requireIndexes');

    var gotMetadataRecord = false;
    var handledOptions = {
        'requireIndexes': false
    };
    var internalOpts;
    var isFirstDataRecord = true;
    var needMetadataRecord = false;
    var optionsSpec = {
        requireIndexes: {
            testNeedHandling: function testNeedHandling(value) {
                return (value === true);
            }
        }
    };
    var optionsToHandle = [];
    var unhandledOptionsErrorEmitted = false;

    optionsToHandle = getUnhandledOptions(options, handledOptions,
        optionsSpec);
    needMetadataRecord = optionsToHandle.length > 0;

    internalOpts = {sendHandledOptions: needMetadataRecord};

    opts = makeOptions(options);
    log = rpc.childLogger(rpcctx, opts);
    res = new EventEmitter();
    req = rpc.rpcCommon({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'findObjects',
        'rpcargs': [ bucket, filter, opts, internalOpts ]
    }, function (err) {
        if (err) {
            res.emit('error', err);
        } else {
            if (!unhandledOptionsErrorEmitted) {
                if (needMetadataRecord && !gotMetadataRecord) {
                    emitUnhandledOptionsError(res, optionsToHandle);
                } else {
                    res.emit('end');
                }
            }
        }

        res.emit('_moray_internal_rpc_done');
    });

    req.on('data', function onObject(msg) {
        var unhandledOptions = [];

        if (isFirstDataRecord && needMetadataRecord) {
            if (Object.hasOwnProperty.call(msg, '_handledOptions')) {
                gotMetadataRecord = true;

                if (msg._handledOptions) {
                    if (msg._handledOptions.indexOf('requireIndexes') !== -1) {
                        handledOptions.requireIndexes = true;
                    }
                }
            }

            unhandledOptions = getUnhandledOptions(options, handledOptions,
                optionsSpec);
            if (unhandledOptions.length > 0) {
                emitUnhandledOptionsError(res, unhandledOptions);
                unhandledOptionsErrorEmitted = true;
                req.removeListener('data', onObject);
            }

            isFirstDataRecord = false;
        } else {
            log.debug({ object: msg }, 'findObjects: record found');
            res.emit('record', msg);
        }
    });

    return (res);
}

function batch(rpcctx, requests, options, callback) {
    assert.object(rpcctx, 'rpcctx');
    assert.arrayOfObject(requests, 'requests');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    for (var i = 0; i < requests.length; i++) {
        var r = requests[i];
        var _s = 'requests[' + i + ']';
        assert.string(r.bucket, _s + '.bucket');
        assert.optionalObject(r.options, _s + '.options');
        assert.optionalString(r.operation, _s + '.operation');
        if (r.operation === 'update') {
            assert.object(r.fields, _s + '.fields');
            assert.string(r.filter, _s + '.filter');
        } else if (r.operation === 'delete') {
            assert.string(r.key, _s + '.key');
        } else if (r.operation === 'deleteMany') {
            assert.string(r.filter, _s + '.filter');
        } else {
            r.operation = r.operation || 'put';
            assert.equal(r.operation, 'put');
            assert.string(r.key, _s + '.key');
            assert.object(r.value, _s + '.value');

            // Allowing differences between the 'value' and '_value' fields is
            // a recipe for disaster.  Any bucket with pre-update actions will
            // wipe out '_value' with a freshly stringified version.  If
            // '_value' contains an invalid JSON string, older version of moray
            // will still accept it, leading to errors when JSON parsing is
            // attempted later during get/find actions.
            // Once it can be ensured that all accessed morays are of an
            // appropriately recent version, this should be removed.
            assert.optionalString(r._value, _s + '._value');
            if (!r._value)
                r._value = JSON.stringify(r.value);

            r = (r.options || {}).headers;
            assert.optionalObject(r, _s + '.options.headers');
        }
    }

    var opts, log;

    opts = makeOptions(options);
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'batch',
        'rpcargs': [ requests, opts ]
    }, function (err, data) {
        if (!err && data.length > 1) {
            err = new VError('expected at most 1 data message, found %d',
                data.length);
        }

        if (err) {
            callback(err);
        } else {
            callback(null, data.length === 0 ? {} : data[0]);
        }
    });
}

function updateObjects(rpcctx, bucket, fields, filter, options, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.object(fields, 'fields');
    assert.string(filter, 'filter');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    opts = makeOptions(options);
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'updateObjects',
        'rpcargs': [ bucket, fields, filter, opts ]
    }, function (err, data) {
        if (!err && data.length > 1) {
            err = new VError('expected at most 1 data message, found %d',
                data.length);
        }

        if (err) {
            callback(err);
        } else {
            callback(null, data.length === 0 ? {} : data[0]);
        }
    });
}

function deleteMany(rpcctx, bucket, filter, options, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.string(filter, 'filter');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    opts = makeOptions(options);
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'deleteMany',
        'rpcargs': [ bucket, filter, opts ]
    }, function (err, data) {
        if (!err && data.length > 1) {
            err = new VError('expected at most 1 data message, found %d',
                data.length);
        }

        if (err) {
            callback(err);
        } else {
            callback(null, data.length === 0 ? {} : data[0]);
        }
    });
}

function reindexObjects(rpcctx, bucket, count, options, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.number(count, 'count');
    assert.ok(count > 0, 'count > 0');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    opts = makeOptions(options);
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'reindexObjects',
        'rpcargs': [ bucket, count, opts ]
    }, function (err, data) {
        if (!err && data.length != 1) {
            err = new VError('expected exactly 1 data message, found %d',
                data.length);
        }

        if (err) {
            callback(err);
        } else {
            var result = data[0];
            log.debug({ 'processed': result.processed },
                'reindexObjects: processed');
            callback(null, result);
        }
    });
}


///--- Helpers

function makeOptions(options, value) {
    var opts = jsprim.deepCopy(options);

    // Defaults handlers
    opts.req_id = options.req_id || libuuid.create();
    opts.etag = (options.etag !== undefined) ? options.etag : options._etag;
    opts.headers = options.headers || {};
    opts.no_count = options.no_count || false;
    opts.sql_only = options.sql_only || false;
    opts.noCache = true;

    // Including the stringified value is redundant, but older versions of
    // moray depend upon the _value field being populated in this way.
    if (value)
        opts._value = JSON.stringify(value);

    if (typeof (options.noCache) !== 'undefined')
        opts.noCache = options.noCache;

    return (opts);
}


///--- Exports

module.exports = {
    putObject: putObject,
    getObject: getObject,
    deleteObject: deleteObject,
    findObjects: findObjects,
    batch: batch,
    updateObjects: updateObjects,
    deleteMany: deleteMany,
    reindexObjects: reindexObjects
};
