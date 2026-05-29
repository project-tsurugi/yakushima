# iterator-based-scan interface

## (old memos for previous prototypes are removed)

## alpha-2 memo

### 2.4

yakushima interface
* 3 関数からなる
    * iscan_open
        * 動作概要
            * 開始端点から探して最初の key の value を返す
        * パラメータ
            * in: 対象storage
            * in: 範囲 (左端点と右端点)
            * in: 向き (順方向(左から右)あるいは逆方向)
            * in: early_abort するかどうか (廃止予定)
            * in: border_node_version_callback (オプショナル)
            * out: return code
            * out: scan_context
            * out: value
            * (alpha 2.4) `function<status(std::string_view storage_name, std::string_view l_key, scan_endpoint l_end, std::string_view r_key, scan_endpoint r_end, bool right_to_left, bool early_abort, iscan_context*& context, void*& value, std::function<bool(node_version64*, node_version64_body)> bnv_cb = nop)>`
        * 動作
            * パラメータ検査で異常があった場合には BADUSAGE を返す
            * 向き指定が順方向指定の場合は、左端点を開始端点、右端点を終了端点とする。逆方向指定の場合は、右端点を開始端点、左端点を終了端点とする
            * 範囲指定で終了端点が開始端点より先にある場合にはパラメータ異常とみなす
            * 順scan かつ -inf 開始の場合は開始端点を "":INCLUSIVE と読み替える
            * 開始端点から最初の key までに間がある場合 (そうでない場合: 端点を範囲に含めていて(INCLUSIVE)、その位置に key が存在した場合):
                * 見つけるまでたどった範囲の border node ひとつひとつにたいして border_node_version_callback を呼ぶ
            * border_node_version_callback が true を返したときは探索を中断して WARN_ABORTED_BY_USER を返す
            * early_abort 指定の場合には yakushima 探索内部で変更を検知したら エラーを返す。そうでない場合は適宜内部 retry する
            * 最初の key が見つかる前に終了端点に到達したら \<OK_SCAN_END, context, N/A> を返す
            * 見つかった場合には次回検索用情報を context に格納して \<OK, context, value> を返す
                * key は返さない (現実装時点での決定, ただし返された context から key を得られる)
                * 情報量的には覚えるのは key だけでよいが next の処理軽減のため border node ポインタや探索経路スタック等の内部情報も格納する
        * 補足: context を返された場合には iscan_close を呼ぶ必要がある
        * 議論: パラメータエラーなどでは context を返さないので、その場合は iscan_close の必要がないとしている
    * iscan_next
        * 動作概要
            * context の 最後の key から探索を再開して次の key を探し、その value を返す
        * パラメータ
            * in: scan_context
            * in: border_node_version_callback (オプショナル)
            * out: return code
            * out: value
        * 動作
            * context の 最後の key を現在位置にセットして探索を再開する
            * 次の key を見つけたら、あるいは終了端点に到達したら探索を中断する
            * 中断するまでたどった範囲の border node ひとつひとつにたいして border_node_version_callback を呼ぶ
            * border_node_version_callback が true を返したときは探索を中断して WARN_ABORTED_BY_USER を返す
            * early_abort 指定の場合には yakushima 探索内部で変更を検知したら エラーを返す。そうでない場合は適宜内部 retry する
            * 次の key が見つかる前に終了端点に到達したら \<OK_SCAN_END, N/A> を返す
            * 見つかった場合には key を context に格納して \<OK, value> を返す
            * context 内部情報を (iscan_open の時と同様に) 更新する
    * iscan_close
        * 動作概要
            * context を解放する
        * パラメータ
            * in: scan_context
        * 動作
            * context が何かを指しているときには解放して、 nullptr とする
            * OK を返す

iscan_context
* std::string full_key()
    * key を得る

* 議論: iscan_open が最初の key を探さないようにするほうが API がきれいなのでそちらも検討していたが、そうしなかった
    * 本案のほうが良い点: context の key が必ず木中のどこかを指す。そのため next はある実在の点から開始できる
    * 別案のほうが良い点: コードがシンプルになる (本案だと、最初のkeyを探すところがnextとほぼ同じであり冗長であるし、パラメータのコールバックも冗長)
