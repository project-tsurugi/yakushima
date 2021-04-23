# todo work

* パーミュテーションの std::sort を廃止する。
* GC に tbb concurrent queue を使うのをやめる。
  * 現在、 garbage が発生したとき、全体で一つの tbb concurrent queue に投入しているが、

  これが大きな性能劣化要因となる。ワーカーそれぞれで garbage の格納場所を複数用意し、
  そこにワーカーは garbage を入れる。 GC マネージャーが舐めるタイミングで格納場所をスイッチし、
  それぞれがロックフリーで機能するようにする。
