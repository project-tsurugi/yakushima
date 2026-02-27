# node version の設計について

Yakushima の node version は Masstree の version から基本的な考え方は踏襲しているが CC からの要請 (Silo 由来の観点) により変更している部分がある.

## insert / delete に関する border node version

背後の要請

* Masstree は 「delete で空いた leaf の位置に insert されると並行で走っている get が不整合した値を読むことが起こり得る」という問題を避けるため,
  delete の後の insert および insert の後の delete で vinsert を増加させ, 検出できるようにしている
    * (TBD) scan/rscan は論文には記載がないが実装にはあるので調べる
* Silo では scan 操作の phantom 検出のため insert は全検出するようにしている

Yakushima では、歴史的経緯により vinsert は実装上は vinsert_delete という名前になっているが,
(2025年時点) 最終的には insert で必ず vinsert を増加させ, delete では vinsert を変更しない.
scan 操作において, 前いなかったものが増えていることを (CC の文脈で) node verify で検出するのに insert の追跡は必須だが
前いたものが消えていることは read verify で実現できるため node version による delete 追跡は必須ではないこと,
および自分の delete による node verify 失敗を防ぐために delete での version 変更を無くしたというのが主な理由である.
(TBD: 自分の delete による node verify 失敗に関しては insert でやっているような対策を入れるという設計方針もありえたのではないか)

この方針は (Masstree 側の要請である) delete + insert 検出を実現でき, また (Silo 側の要請である) insert 全検出を実現できる.

Masstree に比べて劣る点は以下である:

* insert が続く場合に vinsert 更新が増える
    * (vinsert 更新をしない Masstree に比べて) 性能の低下につながる
* 並行で border node を扱う別スレッドは競合検査のために vinsert を使うと delete 操作を検出できない.
    * 競合相手の操作が更新操作である場合には両者が lock を取るので, 適切なタイミングでの lock 操作で実現できる
    * 競合相手の操作が読み取りである場合には楽観的 version 比較で済ませたいが, vinsert だけでなくほかの要素も比較する必要がある

特に最後の 読み取り操作との競合については border node の permutation (60 bits) の比較で変更操作を追跡できると考えている.
permutation だけの比較では delete + insert で ABA 問題的な 問題が起きるが, これは vinsert 比較にて検出できるため,
vinsert と permutation の2つを組み合わせた比較で理論上は変更を漏れなく検出できる.
