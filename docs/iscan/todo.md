### 申し送り事項 (2.4)

alpha 2.x

* SCAN_CONTINUE で findnext から抜けないようにする

alpha 3

* 引数でなくテンプレートパラメータにしたほうが inline 展開が効くらしい ???
* right_to_left を context から取り出すようにしているので テンプレートパラメータでなくなる。
    * `template<bool right_to_left> iscan_context` とかで回避可能?
* `void*` で返しているのを、なにかこう、お上品なものに変える
* 並行更新検出時のリターンコードは、現存のコードのうち一番近そうな WARN_CONCURRENT_OPERATIONS にしたがこれでよいか? エラーコードを新規作成すべきか
    * 構造変化ではなく cb が true を返した場合は違うコードのほうが良くないか
