# iterator-based-scan interface 内部

## alpha-2 memo

iscan_context
* 扱う情報 (抽象的)
    * (openパラメータ由来) モード
        * 進む方向 (順方向/逆方向)
        * early_abort
    * (openパラメータ由来) 終端
    * 最終位置および再開情報
* 実装 (2.5)
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
        * retry 時には stack から key.endpointkind を算出して その位置に findfirst するようにした (mt root から探しなおすので非効率)
            * (リトライでなければ) 一歩(横および上下)進むたびに st.key が更新される. key および last_suffix で構成するので、その参照する要素が確実に一歩一歩更新されていれば正しく retry 出来るはず
                * 下に降りた直後: 現階層の key 8bytes を更新し、子階層の key として min()(/max()) を積んで降りてくる。再開位置は 子階層の 最左(あるいは最右)
                    * forward では 親までのkey : EXCLUSIVE
                    * reverse では sup( x | prefix(x) = 親までのkey) : EXCLUSIVE
                    * min() は 0bytes なので L1+ のすべての slice (最低でも1byte) より小さいことに注意
                * 上に戻った直後: 積まれてあった 8bytes (len=9) の次の位置から再開
                    * forward では sup( x | prefix(x) = key) : INCLUSIVE
                    * backword では key : INCLUSIVE
                * 横(次):
                    * non-suffix-value (len<=8): 最後に見つけた key が登録されている。 これを含まないように再開
                        * last_key : EXCLUSIVE
                    * suffix: 上に戻った直後 と同じ。last_key_suffix を覚えているので full_key で比較することも可能だが、 prefix 部分の範囲に1つしかないことまで見てしまっているわけなので、もう child と同じ扱いにする
            * 不変条件
                * 再開位置は単調増加/単調減少
                * 再開位置が未到達の key を超えない

iscan_state (コード側の文脈で判別され、 iscan_context 内には保存しない)
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
        * 最終 key の位置に戻るまで (別の iscan_state と) 合流してはならない

retry について
* node verify を前提とすると retry している時点ですでに OCC は ERR_CC に落ちるので、retry する意味がないようにも見える
    * つまり、key にたどり着いている時点で その属する border node の range を読んでいる
        * (開始端点上に存在し INCLUSIVE で見つけた最初の key のみがその例外)
    * より細かく言うと最左 lv の左に隙間が無ければ、議論が変わる。 masstree 本家には最左は固定であるという制約があるので影響があるかもしれないが、 yakushima ではその制約がないため 最左lv の左に隙間がある
    * retry 中の border node 走査は cb 対象ではない
* LTX, RTX では snapshot が頑強である必要があり PHANTOM チェックは自力でやるので robust に retry したい
    * 構造変化により前と非連続な位置に戻ってこようが、特に気にする必要はない
