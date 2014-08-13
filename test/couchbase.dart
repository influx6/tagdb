library tagdb.spec;

import 'dart:io';
import 'package:tagdb/server.dart';
import 'package:hub/hub.dart';

void main(){

  var db = TagDB.create('couchbase',{
    'url':'mongo://127.0.0.1/mydb',
    'port':27017
  });

}
