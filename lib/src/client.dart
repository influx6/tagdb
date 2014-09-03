library tagdb;

import 'dart:async';
import 'dart:convert';
import 'package:hub/hub.dart';
import 'package:requestful/client.dart';
import 'package:lawndart/lawndart.dart' as lawn;

part 'base.dart';

class ClientConnectable extends TagDBConnectable{
  MapDecorator conf;
  
   ClientConnectable(m,q): super(m,q){
   this.conf = MapDecorator.useMap(Enums.merge({
      'db': 'defaultdb',
      'id': 'default',
      'estimatedSize': null,
      'authenticate': false,
      'username': null,
      'password': null,
      'url': null,
      'port': null
    },m));
   }
}

class LawnDB extends ClientConnectable{
  lawn.Store db;
  
  static TagQuerable lawnQuery = TagQuerable.create({
    'doc_exists':{
      'condition':(m){
        if(Valids.notExist(m['id'])) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
        return db.db.exists(map['id']);
      })
    },
    'drop_docs':{
      'condition':(m){
        if(Valids.notExist(m['data'])) return false;
        if(m['data'] is! List<String>) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
        return db.db.removeByKeys(map['data']);
      })
    },
    'insert_all':{
      'condition':(m){
        if(Valids.notExist(m['data'])) return false;
        if(m['data'] is! Map) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
        return db.db.batch(map['data']);
      })
    },
    'all_keys':{
      'query': TagQuery.create((db,map){
        Completer res = new Completer(); 
        var stream = db.db.keys(), data = [];
        stream.listen((val){
          data.add(val);
        },onDone: (){
          TagUtil.processData(data,map)
          .then(res.complete,onError: res.completeError);
        }, onError:res.completeError);
        return res.future;
      })
    },
    'get_docs':{
      'condition':(m){
        if(Valids.notExist(m['id'])) return false;
        if(!Valids.isList(m['id'])) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
        Completer res = new Completer(); 
        var stream = db.db.getByKeys(map['id']), data = [];
        stream.listen((val){
          data.add(val);
        },onDone: (){
          TagUtil.processData(data,map)
          .then(res.complete,onError: res.completeError);
        }, onError:res.completeError);
        return res.future;
      })
    },
    'get_doc':{
      'condition':(m){
        if(Valids.notExist(m['id'])) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
        var res = db.db.getByKey(map['id']);
        return res.then((_) => TagUtil.processData(_,map));
      })
    },
    'save_doc':{
      'condition':(m){
        if(Valids.notExist(m['id'])) return false;
        if(Valids.notExist(m['data'])) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
        return db.db.save(map['data'],map['id']);
      })
    },
    'destroy_doc':{
      'condition':(m){
        if(Valids.notExist(m['id'])) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
        return db.db.removeByKey(map['id']);
      })
    },
    'nuke_db':{
      'query': TagQuery.create((db,map){
        return db.db.nuke();
      })
    },
  });
  
  LawnDB(Map co,[TagQuerable q]): super(Funcs.switchUnless(co,{}),Funcs.switchUnless(q,LawnDB.lawnQuery.clone())){
    this.db = new lawn.Store(this.conf.get('db'),this.conf.get('id'),this.conf.core);  
  }  

  Future open(){
    if(this._opened.isCompleted) return this.whenOpen;
    this.db.open().then((d){
      this._opened.complete(d);
    },onError: (e) => this._opened.completeError(e));
    return this.whenOpen;
  }

  Future end(){
    if(!this._closed.isCompleted) return this.whenClosed;
    this.db = null;
    this._closed.complete(true);
    return this.whenClosed;
  }
   
}

class RequestDB extends ClientConnectable{
  Requestful db;
  String toUri;
  
  static TagQuerable requestQuery = TagQuerable.create({
    'repush':{
      'condition': (m){
        if(Valids.notExist(m['to'])) return false;
        if(Valids.notExist(m['method'])) return false;
        if(Valids.notExist(m['data'])) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
        return new Future((){
          var req = db.db.query({
            'method':'put',
            'data': map['data']
          });

          return req.init();
        });
      })
    },
    'push':{
      'condition': (m){
        if(Valids.notExist(m['to'])) return false;
        if(Valids.notExist(m['method'])) return false;
        if(Valids.notExist(m['data'])) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
        return new Future((){
          var req = db.db.query({
            'method':'post',
            'data': map['data']
          });

          return req.init();
        });
      })
    },
    'delete':{
      'condition': (m){
        if(Valids.notExist(m['to'])) return false;
        if(Valids.notExist(m['method'])) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
        return new Future((){
          var req = db.db.query({
            'method':'delete'
          });

          return req.init();
        });
      })
    },
    'get':{
      'condition': (m){
        if(Valids.notExist(m['to'])) return false;
        if(Valids.notExist(m['method'])) return false;
        return true;
      },
      'query': TagQuery.create((db,map){
        return new Future((){
          var req = db.db.query({
            'method':'get'
          });

          req.init().then((d){
            return TagUtil.processData(d['data'],map);
          });
        });
      })
    },
  });
  
  RequestDB(Map co,[TagQuerable q]): super(co,Funcs.switchUnless(q,RequestDB.requestQuery.clone())){
    this._validate(co);
    this.toUri = ([this.conf.get('url'),this.conf.get('port')].join(':'));
    this.db = Requestful.create({
      'to':this.toUri,
      'with':'ajax',
    });
  }

  void _validate(Map co){
    if(Valids.notExist(co['url'])) throw "a url to the location must be provided!";
  }

  Future open(){
    if(this._opened.isCompleted) return this.whenOpen;
    if(Valids.notExist(this.db)) 
      this._opened.completeError(new Exception('Db not ready!'));
    else this._opened.complete(this.db);
    this._closed = new Completer();
    return this.whenOpen;
  }

  Future end(){
    if(this._closed.isCompleted) return this.whenClosed;
    this._opened = new Completer();
    this._closed.complete(true);
    return this.whenClosed;
  }

}

