# node version の設計について

Yakushima の node version は Masstree の version から基本的な考え方は踏襲しているが Shirakami CC からの要請 (Silo 由来の観点) により変更している部分がある.

## insert / delete に関する border node version

背後の要請

* Masstree は 「delete で空いた leaf の位置に insert されると並行で走っている get が不整合した値を読むことが起こり得る」という問題を避けるため,
  delete の後の insert および insert の後の delete で `vinsert` を増加させ, 検出できるようにしている
    * (TBD) scan/rscan での並行変更検出は論文には記載がない
* Silo では scan 操作の phantom 検出のため insert は全検出するようにしている

Yakushima では歴史的経緯により, Masstree 論文における `vinsert` に相当するものは実装上は `vinsert_delete` という名前になっており,
初期実装では全 insert と全 delete で `vinsert_delete` を増加させていたが,
(2025年時点) 最終的には insert で必ず `vinsert_delete` を増加させ, delete では `vinsert_delete` を変更しない.
(そのため, `vinsert` という名前に戻したほうが実態に即している.)

scan 操作において, 前はいなかったものが増えていることを (CC の文脈で) node verify で検出するのに insert の追跡は必須だが
前にいたものが消えていることは read verify で検出できるため node version による delete 追跡は必須ではないこと,
および CC からの GC で delete されたことによる node verify 失敗を防ぐために delete での version 変更を無くしたというのが主な理由である.
(tsurugi-issues#127)

この方針は (Masstree 側の要請である) delete + insert 検出を実現でき, また (Silo 側の要請である) insert 全検出を実現できる.

Masstree に比べて劣る点は以下である:

* insert が続く場合に `vinsert` 更新が増える
    * (`vinsert` 更新をしない Masstree に比べて) 性能の低下につながる
        * lock や書き換え操作をしている時点で重いので version 更新を省略した程度では影響がない?
        * カウンタが早く回ると不利である
* 並行で border node を扱う別スレッドとの競合検査のために `vinsert` を比較しても delete 操作を検出できない.
    * 特に border node を scan している最中に delete されると問題になる
    * 競合相手の操作が更新操作である場合には両者が lock を取るので, 適切なタイミングでの lock 操作で実現できる
    * 競合相手の操作が読み取りである場合には楽観的 version 比較で済ませたいが, `vinsert` だけの比較では検出が不十分である

### delete version がないことによる並行競合検出上の問題

読み取り操作と delete との競合解消のためには, やはり Yakushima としては delete による変更を検出する必要がある.
過剰 phantom 検出を避けるため CC では delete を無視できるように insert と delete は分離した情報として扱いたい.

* border node の `permutation` (64 bits) が delete で変更されるためこれと組み合わせた変更検出に置き換えれば解消できる
    * 試験的機能である iterator based scan ではこれを採用
    * `permutation` だけの比較では delete + insert で値が戻り ABA 的な問題が起きるが,
      1回以上の insert を伴うため, `vinsert` と `permutation` の 2つを組み合わせた比較で理論上は変更を漏れなく検出できる.
    * ただ, 2 words になるため atomic 性は注意が必要
        * `version` → `permutation` → `version` 再確認で補えるが read が 3回に増えることは気にかかる
        * 隣り合うように配置すれば double word atomic read で解消できるかもしれない.
            * (ただし Yakushima 実装は `version` が `base_node` のメンバであり `permutation` が `border_node` のメンバであることにも注意が必要)
* 別の方法として (`vinsert` はそのままにして) delete version を新規導入する手もある.
    * 15個より多くの delete を insert をはさまずに連続することはできないので, 必ず `vinsert` と合わせて比較する前提であれば 4bit でよい
    * `vsplit` から bit を回すのが良いか
* TBD: `inserting` フラグ相当がないことの影響

## 設計ではない点

Yakushima/Shirakami 実装では version 比較で使用するフィールドが Masstree と比べて少ないことがある

* 設計意図は不明 (abort を減らしたいから?)
* `lock` ビットや `inserting`, `splitting` ビットを見ていないことがある. 要調査?
