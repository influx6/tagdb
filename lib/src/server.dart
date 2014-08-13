library tagdb;

import 'dart:core';
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:hub/hub.dart';
import 'package:mongo_dart/mongo_dart.dart' as mongo;
import 'package:couchclient/couchclient.dart' as couch;
import 'package:redis_client/redis_client.dart' as redis;
import 'package:rio/rio.dart';

part 'base.dart';

class MongoDB extends TagDBConnectable{
  mongo.DB db;

  static TagQuerable mongoQuery = TagQuerable.create({
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
    return this.db.open().then((f){
      this._opened.complete(this.db);
      return this.whenOpen;
    },onError:(e) => this._opened.completeError(e));
  }

  Future end(){
    var f = this.db.close();
    if(f is Future) return f.then((j){
      this._closed.complete(j);
      return this.whenClosed;
    },onError:(e) => this._closed.completeError(e));
    this._closed.complete(true);
    return this.whenClosed;
  }

}

class RedisDB extends TagDBConnectable{
  RedisClient client;
  String _url;

  static TagQuerable redisQuery = TagQuerable.create({

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
    RedisClient.connect(this._url).then((cl){
      this.client = cl;
      this._opened.complete(this.client);
    },onError:(e) => this._opened.completeError(e));
    return this.whenOpen;
  }

  Future end(){
    this.client.closed().then((f) => this._closed.complete(f)
        ,onError: (e) => this._closed.completeError(f));
    return this.whenClosed;
  }

}

class CouchBaseDB extends TagDBConnectable{
  couch.Client db;
  Uri uri;

  static TagQuerable couchbaseQuery = TagQuerable.create({

  });
  static create(c,[q]) => new CouchBaseDB(c,q);

  CouchBaseDB(Map co,[TagQuerable q]): super(co,Funcs.switchUnless(q,CouchBase.couchbaseQuery.clone())){
    this.uri = new Uri(conf.get('url'));
    this.db = new CouchClient.connect([this.uri],
        this.conf.get('id'),
        this.conf.get('password'));
  }

  Future open(){
    return this.db.open().then((f){
      this._opened.complete(f);
      return this.whenOpen;
    },onError:(e) => this._opened.completeError(e));
  }

  Future end(){
    var f = this.db.close();
    if(f is Future) return f.then((j){
      this._closed.complete(j);
      return this.whenClosed;
    },onError:(e) => this._closed.completeError(e));
    this._closed.complete(true);
    return this.whenClosed;
  }

}

class CouchDB extends TagDBConnectable{
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
       });
    });
    return this.whenOpen;
  }

  Future end([bool force]){
    force = Funcs.switchUnless(force,true);
    this.client.close(force:force);
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


