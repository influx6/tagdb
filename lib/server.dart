library tagdb.client;

import 'package:tagdb/src/server.dart';
import 'package:hub/hub.dart';

class TagDB{

  static MapDecorator backends = new MapDecorator<String,Function<TagDBConnectable>>.use({
    'redis': (m,[q]) => RedisDB.create(m,q),
    'mongo': (m,[q]) => MongoDB.create(m,q),
    'couch': (m,[q]) => CouchDB.create(m,q),
    'couchbase': (m,[q]) => CouchBaseDB.create(m,q),
  });

  static TagDBConnectable create(String id,Map m,[TagQuerable q]){
    if(TagDB.backends.has(id)) return TagDB.backends.get(id)(m,q);
    return throw "$id is not a valid backend, ${TagDB.backends}";
  }
}
