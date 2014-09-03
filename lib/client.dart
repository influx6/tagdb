library tagdb.client;

import 'package:tagdb/src/client.dart';
import 'package:hub/hub.dart';

class TagDB{

  static MapDecorator backends = new MapDecorator<String,Function<TagDBConnectable>>.use({
    'requestdb':(m,[q]) => new RequestDB(m,q),
    'lawndb':(m,[q]) => new LawnDB(m,q)
  });

  static TagDBConnectable create(String id,Map m,[TagQuerable q]){
    if(TagDB.backends.has(id)) return TagDB.backends.get(id)(m,q);
    return throw "$id is not a valid backend, ${TagDB.backends}";
  }
}
