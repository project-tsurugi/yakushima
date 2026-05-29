# iterator-based-scan interface 内部

## alpha-2 memo

iscan_context
* 扱う情報 (抽象的)
    * (openパラメータ由来) モード
        * 進む方向 (順方向/逆方向)
        * early_abort
    * (openパラメータ由来) 終端
    * 最終位置および再開情報
* 実装 (2.3)
    * 最終位置および再開情報
        * `std::stack<iscan_stack_entry>`
            * where `iscan_stack_entry = < key_tuple, layer_root, border_node, compare_to_end, permutation_iterate_state >`
                * where `permutation_iterate_state = < border_node_version, border_node_permutation, perm_rank >`
        * 議論: 原理的には key_tuple のみで情報が足りていると思われるので layer_root, border_node は冗長ではある
            * layer_root, key_tuple から border_node は算出できる
            * border_node から parent() をたどれば layer_root は算出できる
            * スタック親の border_node とスタック親の key_tuple で layer_root が算出できる
        * 補足: 自レイヤで compare_to_end は end を含みうるかどうか
            * 自分より上の layer ですでに負けているか
        * 議論: compare_to_end は stack の key_tuple を先頭から舐めて end_key と比較すれば再計算できるので冗長である
        * 補足: 再開時および子から戻ったときの early_abort チェックのために permutation_iterate_state を保存している。
        * 議論: early_abort モードでないときには permutation_iterate_state を節約できないか?
        * 補足: border_node_permutation が必要なのは (vinsert_delete で delete を検知できないという) yakushima の制約に基づく (permutation もバージョン比較対象に加える)
            * see: ti#1299
        * 議論: 関数を抜けるポイントは permutation をなめているか、端まで終わったかの二つしかないので、iscan_state 自体は記憶不要。ただ、この二つを区別しなければならないので端まで終わったかのフラグは必要か?
            * → stack size = 0 で判別可能か

iscan_state
* 最初の key の位置を探している
    * 次layer への link にヒット
    * 開始端点の key/value にヒットしたのでユーザに戻す (関数から抜けるポイント)
    * (開始端点位置の key/value にヒットしたが EXCLUSIVE であった、あるいは開始端点位置には key/value がなかったので、次の key を探す)
* 次の key を探している
    * border node の permutation をなめている
        * 範囲の検索を終えた(end に到達した)のでユーザに戻す (関数から抜けるポイント)
        * key/value にヒットしたので、stacktop の 現在位置を更新しユーザに戻す (関数から抜けるポイント)
        * 次layer への link にヒットしたので、stacktop の現在位置を更新し、子layer の新たな スタックフレームを開始する
        * 構造変化検出
            * 同border node からのやりなおしでよい ので i=0 で retry
            * layer root まで戻る
        * permutation をなめ終わった
            * layer の次の border_node へ移動している
            * layer をなめ終わったので上に戻る
    * 構造変化検出により layer root まで戻った
        * root が消されているのでさらに上の layer へ移動
        * findborder で border node に移動
