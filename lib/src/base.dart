part of tagdb;

abstract class TagDBConnectable{
  Switch _capable;
  TagQuerable queries;
  Completer _opened,_closed;

  TagDBConnectable(Map c,this.queries){
    this._capable = Switch.create();
    this._opened = new Completer();
    this._closed = new Completer();

    this._closed.future.then((f){
      this._capable.switchOff();
    });

    this._opened.future.then((f){
      this._capable.switchOn();
    });
  }

  Future open();
  Future end();

  bool get capable => this._capable.on();
  Future get whenOpen => this._opened.future;
  Future get whenClosed => this._closed.future;

  Future query(Map m){
    if(!this.capable) 
      return new Future.error(new Exception('Connection is not alive,first call the open method before this'));
    return this.whenOpen.then((f){
      return this.queries.process(m,this);
    });
  }

}

abstract class Document{
   dynamic get(n);
   dynamic delete(n);
   dynamic update(n,m);
   Map toJson();
   bool get isDirty;
}

class TagQuerable{
  MapDecorator<String,Map> _queries;

  static create([Map m,Map condition]) => new TagQuerable(m,condition);

  TagQuerable([Map<String,TagQuery> m,Map<String,List> conds]){
    this._queries = new MapDecorator<String,Map>.use(Funcs.switchUnless(m,{})); 
  }

  void add(String id,Map q) => this._queries.add(id,q);
  void update(String id,Map q) => this._queries.update(id,q);
  void destroy(String id) => this._queries.destroy(id);
  bool has(String id) => this._queries.has(id);
  bool get(String id) => this._queries.get(id);
  bool forAll(Function n) => this._queries.onAll(n);
  void addAll(Map<String,Map> m) => this._queries.updateAllFrom(m);
  TagQuerable clone() => TagQuerable.create(new Map.from(this._queries.core));

  Future process(Map m,TagDBConnection db){
    var query = this.get(m['@']);
    if(Valids.exist(query)){
        var q = query['query'];
        if(!Valids.exist(q) || q is! TagQuery)
          return new Future.error(new FormatException('query not found! \nQuery: $m'));
        if(query.containsKey('condition')){
          if(Valids.isTrue(query['condition'](m))) return q.process(db,m);
          return new Future.error(new FormatException('query not valid! \nQuery: $m'));
        }
        return q.process(db,m);
    }
  }

}

class TagQuery{
  Function _handler;
  
  static create(n) => new TagQuery(n);

  TagQuery(this._handler);
  Future process(TagDBConnection db,Map m){
    return this._handler(db,m);
  }
}

class TagDocument extends Document{
  MapDecorator maps;
  bool _dirtyFlag;

  static create(m) => new TagDocument(m);

  TagDocument(Map m){
    this.maps = MapDecorator.useMap(m);
  }

  dynamic retrieve(n){
    return this.maps.get(n);
  }

  dynamic delete(n){
    _dirtyFlag = true;
    return this.maps.destroy(n);
  }

  dynamic update(n,val){
    _dirtyFlag = true;
    return this.maps.update(n,val);
  }

  dynamic toJSON(){
    return JSON.encode(this.maps.core);
  }
}


class TagCollection{
  List _collections;
  MapDecorator query;
  Map queryMap, metaData;

  static create(qm,[m]) => new TagCollection(qm,m);

  TagCollection(this.queryMap,[this.metaData]){
    this._collections = new List<TagDocument>();
    this.query = MapDecorator.useMap(this.queryMap);
  }

  int get size => this._collections.length;

  void addDoc(Map m){
    this.add(TagDocument.create(m));
  }

  void add(TagDocument doc){
    if(this._collections.contains(doc)) return;
    this._collections.add(doc);
  }

  void addAll(List<TagDocument> docs){
    this._collections.addAll(docs);
  }

  dynamic deleteIndex(int index){
    return this._collections.removeAt(index);
  }

  dynamic deleteDoc(TagDocument doc){
    var ind = this._collections.indexOf(doc);
    if(ind != null) this.deleteIndex(ind);
    return index;
  }

  dynamic toJSON(){
    return JSON.encode({
      'isTagCollection': true,
      'query': this.queryMap,
      'metaData': this.metaData,
      'collection': this._collections.map((n) => n.toJSON()).toList()
    });
  }

  String toString(){
    return this._collections.toString();
  }
}


class TagUtil{

  static Future processData(dynamic data,[Map meta,Completer completer]){
    var comp = completer != null ? completer : new Completer();
    var col = TagCollection.create(meta);

    if(Valids.notExist(data)){
      comp.complete(null);
      return comp.future;
    }

    Funcs.when((Valids.isMap(data) && !data.containsKey('isTagCollection')),(){
      db.addDoc(data);
      comp.complete(col);
    },(){

      Funcs.when((Valids.isMap(data) && data.containsKey('isTagCollection')),(){
              var meta = data['meta'], query = data['query'], collection = data['collection'];
              Enums.eachAsync(collection,(e,i,o,fn){
                  col.addDoc(e);
                  return fn(null);
              },(_,err){
                  comp.complete(col);
              });
        },(){

          Funcs.when(Valids.isString(data),(){
            col.addDoc({'val': data});
            comp.complete(col);

          },(){

              Funcs.when(Valids.isInt(data),(){
                col.addDoc({'val': data});
                comp.complete(col);

              },(){

                  Funcs.when(Valids.isNum(data),(){
                    col.addDoc({'val': data});
                    comp.complete(col);

                  },(){

                      Funcs.when(Valids.isList(data),(){
                        col.addDoc({'val': data});
                        comp.complete(col);

                      },(){
                        col.addDoc({'val': data});
                        comp.complete(col);
                      });

                  });

              });

          });

      });

    });

    return comp.future;
  }
}
