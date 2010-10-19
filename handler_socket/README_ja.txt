-このプロジェクトについて
ahiguti氏が開発したMySQL plugin「HandlerSocket」
http://github.com/ahiguti/HandlerSocket-Plugin-for-MySQL
のJava版クライアントライブラリです。

HanlerSocketが備える４つのコマンドをJava経由で操作可能です。
-open_index
-find
-find_modify
-insert

※HanlerSocketの詳細仕様についてはHandlerSocketのドキュメントを参照ください。
http://github.com/ahiguti/HandlerSocket-Plugin-for-MySQL/tree/master/docs-ja/


-必要な動作環境
--Java
Generics等を使用しているためJava 1.5以上
--JUnit4.x(test時）


-クラス説明
-- jp.ndca.handlersocket.HandlerSocket
HandlerSocketへの接続〜コマンド操作〜レスポンス解析〜切断 の一連の処理を司るクラスです。
使用方法は後述の「動作方法」もしくは実際のソースを参照ください。

-- jp.ndca.handlersocket.HandlerSocketResult
HandlerSocketのレスポンス結果を表現するBeanクラスです。


-動作方法
--テストクラスの動作
　src/test/java
配下に、テスト用のクラス
　jp.ndca.handlersocket.HandlerSocketTest
を用意しています。実際にHandlerSocket pluginがインストールされているMySQL環境に接続をしてテストを実施します。

テスト実行時に、JavaVM変数に以下のパラメータを指定する必要があります。
 - host : MySQLのhost名もしくはipアドレス
 - port : HandlerSocket pluginのポート番号
 - id : インデックスに割り振るID（HandlerSocketの仕様を参考のこと）
 - db : 対象とするデータベース名
 - table : 対象とするテーブル名
 - fieldList : 対象とするフィールドのリスト（HandlerSocketの仕様を参考のこと）
 - index : 対象とするインデックス名（HandlerSocketの仕様を参考のこと）
 - size : 処理するレコード数
 - loop : ループの回数（ex. size=10000/loop=100 の場合、100件づつの処理を100回繰り返す。(合計10,000件の処理))
 - verbose : 詳細な処理ログを標準出力に表示するかどうか。デフォルトはfalse
 
パラメータの指定例は以下です。
　−Dhost=localhost -Dport=9999 -Did=1 -Ddb=hstest -Dtable=hstest_table1 -DfieldList=k,v -Dindex=PRIMARY -Dsize=10000 -Dloop=100 -Dverbose=true

なお、当該テストの実施前に、事前にテストテーブルを作成する必要があります。
以下のようなテーブルを実施対象のMySQLに予め作成してください。
--------------------------------------------------
CREATE TABLE `hstest.hstest_table1` (
  `k` int(11) NOT NULL,
  `v` varchar(32) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`k`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin
--------------------------------------------------

テストを動作させる場合はテストクラスの直接実行か
　maven test
で動作すると思います。

--HanlerSocketクラスの使用
手順としては以下となります
  1.HanlerSokcetクラスのインスタンス作成 new HandlerSocket()
  2.接続
  3.open_indexコマンド実施 ※実際はexecuteまでは実行されない
  4.各種データ操作コマンドの実施(find/find_modify/insert) ※実際はexecuteまでは実行されない
  5.executeコマンドでコマンドの発行。レスポンスデータの取得
  6.以降「4~5」の処理を繰り返す。
  7.すべて終了したら切断

以下のサンプルソースを参考にしてください。
--------------------------------------------------
HandlerSocket hs = new HandlerSocket();
hs.open(host, port); //接続

hs.command().openIndex(id, db, table, index, fieldList);//open_indexコマンド
String[] values = new String[]{"1", "dummy"};
hs.command().insert(id, keys);//insertコマンド
List<HandlerSocketResult> insertResults = hs.execute();//コマンドの実行


String[] keys = new String[]{"1"};
hs.command().find(id, keys);//findコマンド
List<HandlerSocketResult> findResults =  hs.execute();//コマンドの実行


String[] updateValues = new String[]{"1", "updatedDummy"};
hs.command().findModifyUpdate(id, keys, "=", "1", "0", updateValues);//find_modify(update)コマンド
List<HandlerSocketResult> updateResults =  hs.execute();//コマンドの実行


hs.command().findModifyDelete(id, keys, "=", "1", "0");//find_modify(delete)コマンド
List<HandlerSocketResult> Results =  hs.execute();//コマンドの実行

hs.close();
--------------------------------------------------
