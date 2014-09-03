library tagdb;

import 'dart:core';
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:ds/ds.dart' as ds;
import 'package:hub/hub.dart';
import 'package:mongo_dart/mongo_dart.dart' as mongo;
import 'package:couchclient/couchclient.dart' as couch;
import 'package:redis_client/redis_client.dart' as redis;

part 'base.dart';

class ServerConnectable extends TagDBConnectable{
  MapDecorator conf;
  
   ServerConnectable(m,q): super(m,q){
    this.conf = MapDecorator.useMap(Enums.merge({
      'id': 'default',
      'authenticate': false,
      'ssl': false,
      'digest': false,
      'useUrl': true,
      'username': null,
      'realm': null,
      'password': null,
      'cert': null,
      'certkey':null,
      'url': null,
      'authurl': null,
      'port': 0,
      'path':''
    },m));
   }
}

class CouchBaseDB extends ServerConnectable{
  couch.Client db;
  Uri uri;

  static TagQuerable couchbaseQuery = TagQuerable.create({
    'getAndLock':{
      'condition':(map){
        if(Valids.notExist(map['id'])) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
        if(Valids.exist(map['locktime'])){
           return db.db.getAndLock(map['id'],cas: map['locktime']);
        }
        return db.db.getAndLock(map['id']);
      })
    },
    'unlock':{
      'condition':(map){
        if(Valids.notExist(map['id'])) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
        if(Valids.exist(map['cas'])){
           return db.db.unlock(map['id'],cas: map['cas']);
        }
        return db.db.unlock(map['id']);
      })
    },
    'keyStats':{
      'condition':(map){
        if(Valids.notExist(map['id'])) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
        return db.db.keyStats(map['id']);
      })
    },
    'observe':{
      'condition':(map){
        if(Valids.notExist(map['id'])) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
        if(Valids.exist(map['cas'])){
           return db.db.observe_poll(map['id'],cas: map['cas']);
        }
        return db.db.observe_poll(map['id']);
      })
    },
    'observe_poll':{
      'condition':(map){
        if(Valids.notExist(map['id'])) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
        if(Valids.exist(map['cas'])){
           return db.db.observe_poll(map['id'],cas: map['cas']);
        }
        return db.db.observe_poll(map['id']);
      })
    },
    'drop_doc':{
      'condition':(map){
        if(Valids.notExist(map['id'])) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
         var comp = new Completer();
         db.db.delete(map['id'],(n){
            comp.complete(n);
         });
         return comp.future;
      })
    },
    'get_doc':{
      'condition':(map){
        if(Valids.notExist(map['id'])) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
         return db.db.get(map['id']).then((f){
           return TagUtil.processData(UTF8.decode(f.data),map);
         });
      })
    },
    'add_doc':{
      'condition':(map){
        if(Valids.notExist(map['id'])) return false;
        if(Valids.notExist(map['data'])) return false;
        if(Valids.not(Valids.isMap(map['data']))) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
         return db.db.set(map['id'],map['data']);
      })
    },
  });
  
  static create(c,[q]) => new CouchBaseDB(c,q);

  CouchBaseDB(Map co,[TagQuerable q]): super(co,Funcs.switchUnless(q,CouchBase.couchbaseQuery.clone())){
    this.uri = new Uri(conf.get('url'));
    this.db = new CouchClient.connect([this.uri],
        this.conf.get('id')/*id is the same as bucket*/,
        this.conf.get('password'));
  }

  Future open(){
    if(this._opened.isCompleted) return this.whenOpen;
    return this.db.open().then((f){
      this._opened.complete(f);
      this._closed = new Completer();
      return this.whenOpen;
    },onError:(e) => this._opened.completeError(e));
  }

  Future end(){
    if(this._closed.isCompleted) return this.whenClosed;
    var f = this.db.close();
    if(f is Future) return f.then((j){
      this._opened = new Completer();
      this._closed.complete(j);
      return this.whenClosed;
    },onError:(e) => this._closed.completeError(e));
    this._closed.complete(true);
    return this.whenClosed;
  }

}

class RedisDB extends ServerConnectable{
  RedisClient client;
  String _url;

  static Completer makeZSets(List<Map> data){
    var comp = new Completer(), sets = [];
    Enums.eachAsnc(data,(e,i,o,fn){
      try{
        sets.add(new redis.ZSetEntry(i,e));
      }catch(e){
        comp.completeError(e);
        return fn(e);
      }
      return fn(null);
    },(_,err){
       if(Valids.notExist(err)) com.complete(sets);
    });
    return comp.future;
  }

  static TagQuerable redisQuery = TagQuerable.create({
      'sort':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
           if(Valids.exist(m['optionals']) && Valids.isMap(m['optionals'])){
             return Funcs.dartApply(db.db.sort,[m['key']],m['optionals']);
           }
           return db.db.sort(m['key']);
        })
      },
      'zinterstore':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('sets')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
           if(Valids.exist(m['optionals']) && Valids.isMap(m['optionals'])){
             return Funcs.dartApply(db.db.zinterstore,[m['key'],m['sets']],m['optionals']);
           }
           return db.db.zinterstore(m['key'],m['sets']);
        })
      },
      'zunionstore':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('sets')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
           if(Valids.exist(m['optionals']) && Valids.isMap(m['optionals'])){
             return Funcs.dartApply(db.db.zunionstore,[m['key'],m['sets']],m['optionals']);
           }
           return db.db.zunionstore(m['key'],m['sets']);
        })
      },
      'zscore':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.zscore(map['key'],map['data']);
        })
      },
      'zcard':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.zcard(map['key']);
        })
      },
      'zremrangebyscore':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(m.containsKey('max')){
            if(m['max'] is! int) return false;
          }
          if(m.containsKey('min')){
            if(m['min'] is! int) return false;
          }
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.zremrangebyscore(map['key'],max: map['max'],min: map['min']);
        })
      },
      'zremrangebyrank':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('from')) return false;
          if(!m.containsKey('to')) return false;
          if(m['from'] is! int) return false;
          if(m['to'] is! int) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.zremrangebyrank(map['key'],map['from'],map['to']);
        })
      },
      'zrevrangebyscore':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(m.containsKey('max')){
            if(m['max'] is! int) return false;
          }
          if(m.containsKey('min')){
            if(m['min'] is! int) return false;
          }
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.zrevrangebyscore(map['key'],min: map['min'],max: map['max'],withScores: true, maxExclusive: true);
        })
      },
      'zrangebyscore':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(m.containsKey('max')){
            if(m['max'] is! int) return false;
          }
          if(m.containsKey('min')){
            if(m['min'] is! int) return false;
          }
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.zrangebyscore(map['key'],min: map['min'],max: map['max'],withScores: true, maxExclusive: true);
        })
      },
      'zrevrange':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('from')) return false;
          if(!m.containsKey('to')) return false;
          if(m['from'] is! int) return false;
          if(m['to'] is! int) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.zrevrange(map['key'],map['from'],map['to'],withScores: true);
        })
      },
      'zrange':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('from')) return false;
          if(!m.containsKey('to')) return false;
          if(m['from'] is! int) return false;
          if(m['to'] is! int) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.zrange(map['key'],map['from'],map['to']);
        })
      },
      'zrevrank':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          if(m['data'] is! num) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.zrevrank(map['key'],map['data']);
        })
      },
      'zrank':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.zrank(map['key'],map['data']);
        })
      },
      'zincrby':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('index')) return false;
          if(!m.containsKey('value')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.zincrby(map['key'],map['index'],map['value']);
        })
      },
      'zmrem':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          if(!Valids.isList(m['data'])) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.zmrem(map['key'],map['data']);
        })
      },
      'zsrem':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          if(!Valids.isMap(m['data'])) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.zsrem(map['key'],map['data']);
        })
      },
      'zsadd':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          if(!m.containsKey('index')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.zsadd(map['key'],map['index'],map['data']);
        })
      },
      'zadd':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          if(!Valids.isMap(m['data'])) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          var data = map['data'], key = map['key'];
          return RedisDB.makeZSets(data).then((zs){
              db.db.zadd(key,zs);
          });
        })
      },
      'brpoplpush':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('to')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.brpoplpush(map['key'],m['to']);
        })
      },
      'brpop':{
        'condition':(m){
          if(!m.containsKey('keys')) return false;
          if(!Valids.isList(map['keys'])) return false;
          if(!m.containsKey('timeout')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.brpop(map['keys'],m['timeout']);
        })
      },
      'blpop':{
        'condition':(m){
          if(!m.containsKey('keys')) return false;
          if(!Valids.isList(map['keys'])) return false;
          if(!m.containsKey('timeout')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.blpop(map['keys'],m['timeout']);
        })
      },
      'rpoplpush':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.rpoplpush(map['key'],m['data']).then((r) => TagUtil.processData(r,map));
        })
      },
      'rpop':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.rpop(map['key']);
        })
      },
      'lpop':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.lpop(map['key']);
        })
      },
      'lset':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('index')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.lset(map['key'],map['index'],map['data']);
        })
      },
      'linsert':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          if(!Valids.isList(m['data'])) return false;
          if(m['data'].length < 3) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.linsert(map['key'],map['data'][0],map['data'][1],map['data'][2]);
        })
      },
      'lindex':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('index')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.lindex(map['key'],map['index']);
        })
      },
      'llen':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.llen(map['key']);
        })
      },
      'lrem':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('index')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.lrem(map['key'],map['index'],map['data']);
        })
      },
      'ltrim':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('from')) return false;
          if(!m.containsKey('to')) return false;
          if(m['from'] is! int) return false;
          if(m['to'] is! int) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.ltrim(map['key'],map['from'],map['to']);
        })
      },
      'rpushx':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.rpushx(map['key'],map['data']);
        })
      },
      'lpushx':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.lpushx(map['key'],map['data']);
        })
      },
      'lrange':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.lrange(map['key']);
        })
      },
      'lpush':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.lpush(map['key'],map['data']);
        })
      },
      'rpush':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          if(m['data'] is! List) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.rpush(map['key'],map['data']);
        })
      },
      'hgetall':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.hgetall(map['key']);
        })
      },
      'hvals':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.hvals(map['key']);
        })
      },
      'hkeys':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.hkeys(map['key']);
        })
      },
      'hlen':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.hlen(map['key']);
        })
      },
      'hexists':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('field')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.hexists(map['key'],map['field']);
        })
      },
      'hdel':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('field')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.hdel(map['key'],map['field']);
        })
      },
      'hmget':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('sets')) return false;
          if(m['sets'] is! List) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.hmget(map['key'],map['sets']);
        })
      },
      'hget':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('field')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.hget(map['key'],map['field']);
        })
      },
      'hincrbyfloat':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('field')) return false;
          if(!m.containsKey('value')) return false;
          if(m['value'] is! num) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.hincrbyfloat(map['key'],map['field'],map['value']);
        })
      },
      'hincrby':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('field')) return false;
          if(!m.containsKey('value')) return false;
          if(m['value'] is! int) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.hincrby(map['key'],map['field'],map['value']);
        })
      },
      'hmset':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          if(m['data'] is! Map) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.hmset(map['key'],map['data']);
        })
      },
      'hset':{
        'condition':(m){
          if(!m.containsKey('hashkey')) return false;
          if(!m.containsKey('value')) return false;
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.hset(map['hashkey'],map['key'],map['value']);
        })
      },
      'srandmember':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('index')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.srandmember(map['key'],map['index']);
        })
      },
      'sdiff':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('sets')) return false;
          if(!Valids.isList(m['sets'])) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.sdiff(map['key'],map['sets']);
        })
      },
      'sunionstore':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('sets')) return false;
          if(!Valids.isList(m['sets'])) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.sunionstore(map['key'],map['sets']);
        })
      },
      'sunion':{
        'condition':(m){
          if(!m.containsKey('sets')) return false;
          if(!Valids.isList(m['sets'])) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.sunion(map['sets']);
        })
      },
      'sinterstore':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('sets')) return false;
          if(!Valids.isList(m.containsKey('sets'))) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.sinterstore(map['key'],map['sets']);
        })
      },
      'sinter':{
        'condition':(m){
          if(!m.containsKey('sets')) return false;
          if(!Valids.isList(m['sets'])) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.sinter(map['sets']);
        })
      },
      'sismembers':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.sismember(map['key'],map['data']);
        })
      },
      'scard':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.scard(map['key']);
        })
      },
      'smove':{
        'condition':(m){
          if(!m.containsKey('from')) return false;
          if(!m.containsKey('to')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.smove(map['from'],map['to'],['data']);
        })
      },
      'spop':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.spop(map['key']);
        })
      },
      'srem':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.srem(map['key'],map['data']);
        })
      },
      'sadd':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!Valids.isList(m['key'])) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          var sets = new Set.from(map['data']);
          return db.db.sadd(map['key'],set);
        })
      },
      'smembers':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.smembers(map['key']);
        })
      },
      'expireatweeks':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('weeks')) return false;
          if(!Valids.isInt(m.containsKey('weeks'))) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          var days = new Duration(days: map['weeks'] * 7);
          return db.db.expireat(map['key'],new DateTime.now().add(days));
        })
      },
      'expireatdays':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('days')) return false;
          if(!Valids.isInt(m.containsKey('days'))) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          var days = new Duration(days: map['days']);
          return db.db.expireat(map['key'],new DateTime.now().add(days));
        })
      },
      'pexpire':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.pexpire(map['key'],map['data']);
        })
      },
      'expire':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.expire(map['key'],map['data']);
        })
      },
      'renamenx':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.renamenx(map['key'],map['data']);
        })
      },
      'rename':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.rename(map['key'],map['data']);
        })
      },
      'setrange':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('start')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.getrange(map['key'],map['start'],map['data']);
        })
      },
      'getrange':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('start')) return false;
          if(!m.containsKey('end')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.getrange(map['key'],map['start'],map['end']);
        })
      },
      'append':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.append(map['key'],map['data']);
        })
      },
      'strlen':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.strlen(map['key']);
        })
      },
      'msetnx':{
        'condition':(m){
          if(!m.containsKey('data')) return false;
          if(!Valids.isMap(m.containsKey('data'))) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.msetnx(map['data']);
        })
      },
      'mset':{
        'condition':(m){
          if(!m.containsKey('data')) return false;
          if(!Valids.isMap(m.containsKey('data'))) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.mset(map['data']);
        })
      },
      'persist':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.persist(map['key']);
        })
      },
      'mdel':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!Valids.isList(m.containsKey('key'))) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.mdel(map['key']);
        })
      },
      'del':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.del(map['key']);
        })
      },
      'exists':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.exits(map['key']);
        })
      },
      'pttl':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.pttl(map['key']).then((val) => TagUtil.processData(val,map));
        })
      },
      'ttl':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.ttl(map['key']).then((val) => TagUtil.processData(val,map));
        })
      },
      'psetex':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('index')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.psetex(map['key'],map['index'],map['data']);
        })
      },
      'setex':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('index')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
          return db.db.setex(map['key'],map['index'],map['data']);
        })
      },
      'mget':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!Valids.isList(m.containsKey('key'))) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.mget(map['key']).then((val) => TagUtil.processData(val,map));
        })
      },
      'keys':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.keys(map['key']);
        })
      },
      'getbit':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('index')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.getbit(map['key'],map['index']);
        })
      },
      'setbit':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('index')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.setbit(map['key'],map['index'],map['data']);
        })
      },
      'type':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.type(map['key']);
        })
      },
      'echo':{
        'condition':(m){
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.echo(map['data']);
        })
      },
      'randomKey':{
        'query': TagQuery.create((db,map){
            return db.db.randomkey();
        })
      },
      'lastSaveDiff':{
        'query': TagQuery.create((db,map){
            return db.db.lastsave.difference(DateTime.now());
        })
      },
      'ping':{
        'query': TagQuery.create((db,map){
            return db.db.ping();
        })
      },
      'lastSave':{
        'query': TagQuery.create((db,map){
            return db.db.lastsave;
        })
      },
      'flushdb':{
        'query': TagQuery.create((db,map){
            return db.db.flushdb();
        })
      },
      'dbsize':{
        'query': TagQuery.create((db,map){
            return db.db.dbsize;
        })
      },
      'incrbyfloat':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          if(!Valids.isNum(m.containsKey('data'))) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            var key = map['key'];
            return db.db.incrbyfloat(key,map['data']);
        })
      },
      'decrby':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            var key = map['key'];
            return db.db.decrby(key,map['data']);
        })
      },
      'decr':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            var key = map['key'];
            return db.db.decr(key);
        })
      },
      'incrby':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            var key = map['key'];
            return db.db.incrby(key,map['data']);
        })
      },
      'incr':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            var key = map['key'];
            return db.db.incr(key);
        })
      },
      'select':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            var key = map['key'];
            return db.db.select(key);
        })
      },
      'getset':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            var key = map['key'], data = map['data'];
            return db.db.getset(key,data).then((val){
              return TagUtil.processData(val,map);
            });
        })
      },
      'set':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            var key = map['key'], data = map['data'];
            return db.db.set(key,data);
        })
      },
      'get':{
        'condition':(m){
          if(!m.containsKey('key')) return false;
          return true;
        },
        'query': TagQuery.create((db,map){
            return db.db.get(map['key']).then((val){
                return TagUtil.processData(val,map);
            });
        })
      },
      'save_doc':{
        'query': TagQuery.create((db,map){
            var q = new Map.from(map);
            q['key'] = 'set';
            return db.query(q);
        })
      },
      'drop_doc':{
        'query': TagQuery.create((db,map){
            var q = new Map.from(map);
            q['key'] = 'delete';
            return db.query(q);
        })
      },
      'update_doc':{
        'query': TagQuery.create((db,map){
            var q = new Map.from(map);
            q['key'] = 'set';
            return db.query(q);
        })
      },
      'get_doc':{
        'query': TagQuery.create((db,map){
            var q = new Map.from(map);
            q['key'] = 'get';
            return db.query(q);
        })
      },
  });
  static create(c,[q]) => new RedisDB(c,q);

  RedisDB(Map c,[TagQuerable q]): super(c,Funcs.switchUnless(q,RedisDB.redisQuery.clone())){
    if(this.conf.get('authenticate')){
      this._url = [this.conf.get('password'),'@',this.conf.get('url'),':',this.conf.get('port'),'/',this.conf.get('path')].join('');
    }else{
      this._url = [this.conf.get('url'),':',this.conf.get('port'),'/',this.conf.get('path')].join('');
    }
  }

  Future open(){
    if(this._opened.isCompleted) return this.whenOpen;
    RedisClient.connect(this._url).then((cl){
      this.client = cl;
      this._closed = new Completer();
      this._opened.complete(this.client);
    },onError:(e) => this._opened.completeError(e));
    return this.whenOpen;
  }

  Future end(){
    if(this._closed.isCompleted) return this.whenClosed;
    this.client.closed().then((f){ 
      this._opened = new Completer();
      this._closed.complete(f);
    }
    ,onError: (e) => this._closed.completeError(f));
    return this.whenClosed;
  }

}

class MongoDB extends ServerConnectable{
  mongo.DB db;

  static TagQuerable mongoQuery = TagQuerable.create({
    'get_doc':{
      'query': TagQuery.create((db,map){
          var q = new Map.from(map);
          q['id'] = "find";
          return db.query(q);
      })
    },
    'save_doc':{
      'query': TagQuery.create((db,map){
          var q = new Map.from(map);
          q['id'] = "insert";
          return db.query(q);
      })
    },
    'update_doc':{
      'query': TagQuery.create((db,map){
          var q = new Map.from(map);
          q['id'] = "update";
          return db.query(q);
      })
    },
    'drop_doc':{
      'query': TagQuery.create((db,map){
          var q = new Map.from(map);
          q['id'] = "drop";
          return db.query(q);
      })
    },
    'all': {
          'condition':(m){
            if(m.containsKey('collection')) return true;
          },
          'query':TagQuery.create((db,map){
          return new Future((){
            var tcol = TagCollection.create(map);
            var col = db.db.collection(map['collection']);
            if(Valids.notExist(col)) return "$map collection not found!";
            var find = col.find();
            return find.forEach((n){
              print('total item each $n');
              var id = n['_id'];
              n['_id'] = id.toHexString();
              tcol.addDoc(n);
            }).then((n){
              return tcol;
            });
          });
        })
    },
    "find": {
        'condition':(m){
          if(!m.containsKey('collection')) return false;
          if(m.containsKey('criteria')){
            if(m['criteria'] is! List) return false;
          }
          return true;
        },
        'query':TagQuery.create((db,map){
        return new Future((){
          var tcol = TagCollection.create(map);
          var col = db.db.collection(tcol.query.get('collection'));
          if(Valids.notExist(col)) return "$map collection not found!";
          var criteria = tcol.query.get('criteria');
          var find = Funcs.dartApply(col.find,criteria);
          return find.forEach((n){
            var id = n['_id'];
            n['_id'] = id.toHexString();
            tcol.addDoc(n);
          }).then((n){
            return tcol;
          });
        });
      }) 
    },
    "findOne": {
        'condition':(m){
          if(!m.containsKey('collection')) return false;
          if(m.containsKey('criteria')){
            if(m['critera'] is! List<Map>) return false;
          }
          return true;
        },
        'query':TagQuery.create((db,map){
        return new Future((){
          var tcol = TagCollection.create(map);
          var col = db.db.collection(tcol.query.get('collection'));
          if(Valids.notExist(col)) return "$map collection not found!";
          var criteria = Funcs.switchUnless(tcol.query.get('criteria'),[]);
          var find = Funcs.dartApply(col.findOne,criteria);
          return find.then((n){
            var id = n['_id'];
            n['_id'] = id.toHexString();
            tcol.addDoc(n);
            return tcol;
          });
        });
      })
    },
    "insert": {
        'condition':(m){
          if(!m.containsKey('collection')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query':TagQuery.create((db,map){
        return new Future((){
          var tcol = TagCollection.create(map);
          var col = db.db.collection(tcol.query.get('collection'));
          if(Valids.notExist(col)) return "$map collection not found!";
          return col.insert(tcol.query.get('data'));
        });
      })
    },
    "insertAll": {
        'condition':(m){
          if(!m.containsKey('collection')) return false;
          if(m.containsKey('data')){
            if(m['data'] is! List) return false;
          }
          return true;
        },
        'query':TagQuery.create((db,map){
        return new Future((){
          var tcol = TagCollection.create(map);
          var col = db.db.collection(tcol.query.get('collection'));
          if(Valids.notExist(col)) return "$map collection not found!";
          return col.insertAll(tcol.query.get('data'));
        });
      })
    },
    "save": {
        'condition':(m){
          if(!m.containsKey('collection')) return false;
          if(!m.containsKey('data')) return false;
          return true;
        },
        'query':TagQuery.create((db,map){
        return new Future((){
          var tcol = TagCollection.create(map);
          var col = db.db.collection(tcol.query.get('collection'));
          if(Valids.notExist(col)) return "$map collection not found!";
          var data = tcol.query.get('data');
          return col.save(data);
        });
      })
    },
    "update": {
        'condition':(m){
          if(!m.containsKey('collection')) return false;
          if(m.containsKey('criteria')){
            if(m['criteria'] is! List<Map>) return false;
          }
          return true;
        },
        'query':TagQuery.create((db,map){
        return new Future((){
          var tcol = TagCollection.create(map);
          var col = db.db.collection(tcol.query.get('collection'));
          if(Valids.notExist(col)) return "$map collection not found!";
          var criteria = tcol.query.get('criteria');
          return Funcs.dartApply(col.update,criteria.sublist(0,2),Enums.third(criteria));
        });
      })
    },
    "delete": {
        'condition':(m){
          if(!m.containsKey('collection')) return false;
          if(m.containsKey('criteria')){
            if(m['critera'] is! List<Map>) return false;
          }
          return true;
        },
        'query':TagQuery.create((db,map){
        return new Future((){
          var tcol = TagCollection.create(map);
          var col = db.db.collection(tcol.query.get('collection'));
          if(Valids.notExist(col)) return "$map collection not found!";
          var criteria = tcol.query.get('criteria');
          return Funcs.dartApply(col.remove,criteria);
        });
      })
    },
    "drop": {
        'condition':(m){
          if(!m.containsKey('collection')) return false;
          return true;
        },
        'query':TagQuery.create((db,map){
        return new Future((){
          var tcol = TagCollection.create(map);
          var col = db.db.collection(tcol.query.get('collection'));
          if(Valids.notExist(col)) return "$map collection not found!";
          return col.drop();
        });
      })
    },
  });

  static create(c,[q]) => new MongoDB(c,q);

  MongoDB(Map c,[TagQuerable q]): super(c,Funcs.switchUnless(q,MongoDB.mongoQuery.clone())){
    this.db = new mongo.Db(this.conf.get('url'),this.conf.get('id'));
  }

  Future open(){
    if(this._opened.isCompleted) return this.whenOpen;
    return this.db.open().then((f){
      this._opened.complete(this.db);
      this._closed = new Completer();
      return this.whenOpen;
    },onError:(e) => this._opened.completeError(e));
  }

  Future end(){
    if(this._closed.isCompleted) return this.whenClosed;
    var f = this.db.close();
    this._opened = new Completer();
    if(f is Future) return f.then((j){
      this._closed.complete(j);
      return this.whenClosed;
    },onError:(e) => this._closed.completeError(e));
    this._closed.complete(true);
    return this.whenClosed;
  }

}

class CouchDB extends ServerConnectable{
  HttpClient client;
  HttpClientBasicCrendentials bcert,gcert;
  String _url,_authurl;
  Uri uri,authUri;
 
  static TagQuerable couchQuery = TagQuerable.create();
  static create(c,[q]) => new CouchDB(c,q);

  CouchDB(Map co,[TagQuerable q]): super(co,Funcs.switchUnless(q,CouchDB.couchQuery.clone())){
    this.client =  new HttpClient();
    this.bcert = new HttpClientDigestCredentials(conf.get('username'),conf.get('password'));
    this.gcert = new HttpClientBasicCredentials(conf.get('username'),conf.get('password'));
    this._url = [this.conf.get('url'),':',Funcs.switchUnless(this.conf.get('port'),80)].join('');
    this._authurl = [Funcs.switchUnless(this.conf.get('authurl'),this.conf.get('url')),':',Funcs.switchUnless(this.conf.get('port'),80)].join('');
    this.uri = Uri.parse(this._url);
    this.authUri = Uri.parse(this._authurl);
    this._addQueries();
  }

  String get url => this._url;
  String get authUrl => this._authurl;

  Future open([Function beforeReq]){
    if(this._opened.isCompleted) return this.whenOpen;
    if(this.conf.get('authenticate')){
      var cred = Valids.isTrue(this.conf.get('disgest')) ? this.bcert : this.gcert;
      this.client.addCredentials(this.authUri,this.conf.get('realm'),cred);
    }

    this.client.openUrl('GET',this.uri).then((req){
      if(Valids.exist(beforeReq)) beforeReq(req);
      return req.close();
    },onError:(e) => this._open.completeError(e)).then((res){
       var data = [];
       res.listen((n){
         if(n is List) data.addAll(n);
         else data.add(n);
       },onDone:(){
         this._opened.complete(UTF8.decode(data));
         this._closed = new Completer();
       });
    });
    return this.whenOpen;
  }

  Future end([bool force]){
    if(this._closed.isCompleted) return this.whenClosed;
    force = Funcs.switchUnless(force,true);
    this.client.close(force:force);
    this._opened = new Completer();
    this._closed.complete(force);
    return this.whenClosed;
  }

  Future _collectData(HttpResponse res){
    var ca = new Completer(), data = [];
    res.listen((n){
        data.addAll(n is List ? n : [n]);
    },onDone:(){
      ca.complete(UTF8.decode(data));
    },onError:(e) => ca.completeError(e));
    return ca.future;
  }

  void _addQueries(){
    this.queries.addAll({
        'all_docs':{
          'condition':(m){
              if(!m.containsKey('db')) return false;
              return true;
          },
          'query': TagQuery.create((db,map){
            return this.client.openUrl('GET',Uri.parse([this.url,map['db'],'_all_docs'].join('/')))
            .then((req){
                return req.close();
            }).then((res){
              return this._collectData(res).then((data){
                var json = JSON.decode(data);
                var ft = new Completer();
                var col = TagCollection.create(map);
                var rows = json['rows'];
                Enums.eachAsync(rows,(e,i,o,fn){
                  col.addDoc(e);
                  return fn(null);
                },(_,err){
                  ft.complete(col);
                });
                return ft.future;
              });
            });
          })
        },
        'get_doc':{
          'condition':(m){
              if(!m.containsKey('db')) return false;
              if(!m.containsKey('doc_id')) return false;
              return true;
          },
          'query': TagQuery.create((db,map){
            return this.client.openUrl('GET',Uri.parse([this.url,map['db'],map['doc_id']].join('/')))
              .then((req){
                  return req.close();
              }).then((res){
                var meta = {};
                res.headers.forEach((k,v) => meta[k] = v);
                return this._collectData(res).then((data){
                  var col = TagCollection.create(map,meta);
                  col.addDoc(data);
                  return col;
                });
              });
          })
        },
        'save_doc':{
          'condition':(m){
              if(!m.containsKey('db')) return false;
              if(!m.containsKey('data')) return false;
              return true;
          },
          'query': TagQuery.create((db,map){
              var uuid;
              if(map.containsKey('doc_id')) uuid = map['doc_id'];
              else uuid = Hub.randomStringsets(5,'');
              return this.client.openUrl('PUT',Uri.parse([this.url,map['db'],uuid].join('/')))
              .then((req){
                try{
                  req.headers.contentLength = -1;
                  req.write(JSON.encode(map['data']));
                }catch(e){
                  return e;
                };
                return req.close();
              }).then((res){
                return this._collectData(res);
              });
          })
        },
        'update_doc':{
          'condition':(m){
              if(!m.containsKey('db')) return false;
              if(!m.containsKey('doc_id')) return false;
              if(!m.containsKey('data')) return false;
              return true;
          },
          'query': TagQuery.create((db,map){
              return this.client.openUrl('PUT',Uri.parse([this.url,map['db'],map['doc_id']].join('/')))
              .then((req){
                try{
                  req.headers.contentLength = -1;
                  req.write(JSON.encode(map['data']));
                }catch(e){
                  return e;
                };
                return req.close();
              }).then((res){
                return this._collectData(res);
              });
          })
        },
        'drop_doc':{
          'condition':(m){
              if(!m.containsKey('db')) return false;
              if(!m.containsKey('doc_id')) return false;
              if(!m.containsKey('rev_id')) return false;
              return true;
          },
          'query': TagQuery.create((db,map){
            return this.client.openUrl('DELETE',Uri.parse([this.url,'/',map['db'],'/',map['doc_id'],'?rev=',map['rev_id']].join('').replaceAll('"','')))
              .then((req){
                return req.close();
              }).then((res){
                return this._collectData(res);
              });
          })
        },
        'all_db':{
          'query': TagQuery.create((db,map){
              return this.client.openUrl('GET',Uri.parse([this.url,'_all_dbs'].join('/'))).then((req){
                 return req.close();
              }).then((res){
                return this._collectData(res);
              });
          })
        },
        'create_db':{
          'condition':(m){
              if(!m.containsKey('db')) return false;
              return true;
          },
          'query': TagQuery.create((db,map){
              return this.client.openUrl('PUT',Uri.parse([this.url,map['db']].join('/'))).then((req){
                 return req.close();
              }).then((res){
                return this._collectData(res);
              });
          })
        },
        'drop_db':{
          'condition':(m){
              if(!m.containsKey('db')) return false;
              return true;
          },
          'query': TagQuery.create((db,map){
              return this.client.openUrl('DELETE',Uri.parse([this.url,map['db']].join('/'))).then((req){
                 return req.close();
              }).then((res){
                return this._collectData(res);
              });
          })
        },
    });
  }
}


