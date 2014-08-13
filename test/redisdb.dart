library tagdb.spec;

import 'dart:io';
import 'package:tagdb/server.dart';
import 'package:hub/hub.dart';

void main(){

  var redis = TagDB.create('redis',{
    'url':'127.0.0.1',

  redis.open().then(Funcs.tag('opened redis')),then((f){
    redis.end().then(Funcs.tag('closd redis'));
  });

}
