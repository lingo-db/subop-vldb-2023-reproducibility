module {
  func.func @main() {
    %0 = relalg.basetable  {table_identifier = "title"} columns: {episode_nr => @t::@episode_nr({type = !db.nullable<i32>}), episode_of_id => @t::@episode_of_id({type = !db.nullable<i32>}), id => @t::@id({type = i32}), imdb_id => @t::@imdb_id({type = !db.nullable<i32>}), imdb_index => @t::@imdb_index({type = !db.nullable<!db.string>}), kind_id => @t::@kind_id({type = i32}), md5sum => @t::@md5sum({type = !db.nullable<!db.string>}), phonetic_code => @t::@phonetic_code({type = !db.nullable<!db.string>}), production_year => @t::@production_year({type = !db.nullable<i32>}), season_nr => @t::@season_nr({type = !db.nullable<i32>}), series_years => @t::@series_years({type = !db.nullable<!db.string>}), title => @t::@title({type = !db.string})}
    %1 = relalg.basetable  {table_identifier = "movie_info"} columns: {id => @mi::@id({type = i32}), info => @mi::@info({type = !db.string}), info_type_id => @mi::@info_type_id({type = i32}), movie_id => @mi::@movie_id({type = i32}), note => @mi::@note({type = !db.nullable<!db.string>})}
      %t3 = relalg.basetable  {table_identifier = "title"} columns: {episode_nr => @t2::@episode_nr({type = !db.nullable<i32>}), episode_of_id => @t2::@episode_of_id({type = !db.nullable<i32>}), id => @t2::@id({type = i32}), imdb_id => @t2::@imdb_id({type = !db.nullable<i32>}), imdb_index => @t2::@imdb_index({type = !db.nullable<!db.string>}), kind_id => @t2::@kind_id({type = i32}), md5sum => @t2::@md5sum({type = !db.nullable<!db.string>}), phonetic_code => @t2::@phonetic_code({type = !db.nullable<!db.string>}), production_year => @t2::@production_year({type = !db.nullable<i32>}), season_nr => @t2::@season_nr({type = !db.nullable<i32>}), series_years => @t2::@series_years({type = !db.nullable<!db.string>}), title => @t2::@title({type = !db.string})}
    %2 = relalg.crossproduct %0, %1
    %3 = relalg.basetable  {table_identifier = "info_type"} columns: {id => @it::@id({type = i32}), info => @it::@info({type = !db.string})}
    %4 = relalg.crossproduct %2, %3
    %5 = relalg.selection %4 (%arg0: !tuples.tuple){
      %14 = tuples.getcol %arg0 @t::@id : i32
      %15 = tuples.getcol %arg0 @mi::@movie_id : i32
      %16 = db.compare eq %14 : i32, %15 : i32
      %17 = tuples.getcol %arg0 @it::@id : i32
      %18 = tuples.getcol %arg0 @mi::@info_type_id : i32
      %19 = db.compare eq %17 : i32, %18 : i32
      %20 = tuples.getcol %arg0 @it::@info : !db.string
      %21 = db.constant("genres") : !db.string
      %22 = db.compare eq %20 : !db.string, %21 : !db.string
      %23 = tuples.getcol %arg0 @mi::@info : !db.string
      %24 = db.constant("Crime") : !db.string
      %25 = db.compare eq %23 : !db.string, %24 : !db.string
      %26 = db.and %16, %19, %22, %25 : i1, i1, i1, i1
      tuples.return %26 : i1
    }
    %7 = relalg.basetable  {table_identifier = "title"} columns: {episode_nr => @t1::@episode_nr({type = !db.nullable<i32>}), episode_of_id => @t1::@episode_of_id({type = !db.nullable<i32>}), id => @t1::@id({type = i32}), imdb_id => @t1::@imdb_id({type = !db.nullable<i32>}), imdb_index => @t1::@imdb_index({type = !db.nullable<!db.string>}), kind_id => @t1::@kind_id({type = i32}), md5sum => @t1::@md5sum({type = !db.nullable<!db.string>}), phonetic_code => @t1::@phonetic_code({type = !db.nullable<!db.string>}), production_year => @t1::@production_year({type = !db.nullable<i32>}), season_nr => @t1::@season_nr({type = !db.nullable<i32>}), series_years => @t1::@series_years({type = !db.nullable<!db.string>}), title => @t1::@title({type = !db.string})}
    %8 = relalg.basetable  {table_identifier = "movie_info"} columns: {id => @mi1::@id({type = i32}), info => @mi1::@info({type = !db.string}), info_type_id => @mi1::@info_type_id({type = i32}), movie_id => @mi1::@movie_id({type = i32}), note => @mi1::@note({type = !db.nullable<!db.string>})}
    %9 = relalg.crossproduct %7, %8
    %10 = relalg.basetable  {table_identifier = "info_type"} columns: {id => @it1::@id({type = i32}), info => @it1::@info({type = !db.string})}
    %11 = relalg.crossproduct %9, %10
    %12 = relalg.selection %11 (%arg0: !tuples.tuple){
      %14 = tuples.getcol %arg0 @t1::@id : i32
      %15 = tuples.getcol %arg0 @mi1::@movie_id : i32
      %16 = db.compare eq %14 : i32, %15 : i32
      %17 = tuples.getcol %arg0 @it1::@id : i32
      %18 = tuples.getcol %arg0 @mi1::@info_type_id : i32
      %19 = db.compare eq %17 : i32, %18 : i32
      %20 = tuples.getcol %arg0 @it1::@info : !db.string
      %21 = db.constant("genres") : !db.string
      %22 = db.compare eq %20 : !db.string, %21 : !db.string
      %23 = tuples.getcol %arg0 @mi1::@info : !db.string
      %24 = db.constant("Crime") : !db.string
      %25 = db.compare eq %23 : !db.string, %24 : !db.string
      %26 = db.and %16, %19, %22, %25 : i1, i1, i1, i1
      tuples.return %26 : i1
    }
     %nested = relalg.nested %5,%12 [@t::@id,@t::@title,@t1::@id,@t1::@title] -> [@t1::@title,@t1::@id,@ngramlist::@id] (%stream1, %stream2){
                %ngram_index = subop.create !subop.multimap<[ ngram_ht_1 : !db.string],[ id_ht_1 : i32]>
                %rel1_splitted = subop.nested_map %stream1 [@t::@title](%t, %str){
                    %splitted = subop.generate [@rel1::@ngram({type=!db.string})] {
                            %c0 = arith.constant 0 : index
                            %c1 = arith.constant 1 : index
                            %n = arith.constant 5 : index // n as in n-grams
                            %len64 = db.runtime_call "StringLength" (%str) : (!db.string) -> i64
                            %len = arith.index_castui %len64 : i64 to index
                            %lenLtN = arith.cmpi ult, %len, %n : index
                            scf.if %lenLtN {
                                subop.generate_emit %str : !db.string
                            } else {
                                %lenMn = arith.subi %len, %n : index
                                %lenMnP1 = arith.addi %lenMn,%c1 : index
                                scf.for %i = %c0 to %lenMnP1 step %c1 {
                                  %iP1 = arith.addi %i, %c1 : index
                                  %iPn = arith.addi %i, %n : index
                                  %substr = db.runtime_call "Substring" (%str, %iP1, %iPn) : (!db.string, index,index) -> !db.string
                                  subop.generate_emit %substr : !db.string
                                }

                            }
                            tuples.return
                        }
                    tuples.return %splitted : !tuples.tuplestream
                }
                subop.insert %rel1_splitted %ngram_index :   !subop.multimap<[ ngram_ht_1 : !db.string],[ id_ht_1 : i32]> {@t::@id => id_ht_1, @rel1::@ngram => ngram_ht_1} eq: ([%l],[%r]){
                    %matches = db.compare eq %l : !db.string, %r : !db.string
                    tuples.return %matches : i1
                }
                %rel2_splitted = subop.nested_map %stream2 [@t1::@title,@t1::@id](%t, %str,%currId){
                    %cnt_map = subop.create !subop.hashmap<[ id_ht2 : i32],[ cnt : i32]>
                    %splitted = subop.generate [@rel2::@ngram({type=!db.string})] {
                            %c0 = arith.constant 0 : index
                            %c1 = arith.constant 1 : index
                            %n = arith.constant 5 : index // n as in n-grams
                            %len64 = db.runtime_call "StringLength" (%str) : (!db.string) -> i64
                            %len = arith.index_castui %len64 : i64 to index
                            %lenLtN = arith.cmpi ult, %len, %n : index
                            scf.if %lenLtN {
                                subop.generate_emit %str : !db.string
                            } else {
                                %lenMn = arith.subi %len, %n : index
                                %lenMnP1 = arith.addi %lenMn,%c1 : index
                                scf.for %i = %c0 to %lenMnP1 step %c1 {
                                  %iP1 = arith.addi %i, %c1 : index
                                  %iPn = arith.addi %i, %n : index
                                  %substr = db.runtime_call "Substring" (%str, %iP1, %iPn) : (!db.string, index,index) -> !db.string
                                  subop.generate_emit %substr : !db.string
                                }

                            }
                            tuples.return
                        }
                    %rel2_lookup = subop.lookup %splitted %ngram_index [@rel2::@ngram] :  !subop.multimap<[ ngram_ht_1 : !db.string],[ id_ht_1 : i32]>  @ngramlist::@lookup_ref_list({type=!subop.list<!subop.lookup_entry_ref<!subop.multimap<[ ngram_ht_1 : !db.string],[ id_ht_1 : i32]>>>}) eq: ([%l],[%r]){
                        %matches = db.compare eq %l : !db.string, %r : !db.string
                        tuples.return %matches : i1
                    }
                    %rel2_matches = subop.nested_map %rel2_lookup [@ngramlist::@lookup_ref_list](%t2, %list){
                        %scan_matches = subop.scan_list %list : !subop.list<!subop.lookup_entry_ref<!subop.multimap<[ ngram_ht_1 : !db.string],[ id_ht_1 : i32]>>> @ngramlist::@lookup_ref({type=!subop.lookup_entry_ref<!subop.multimap<[ ngram_ht_1 : !db.string],[ id_ht_1 : i32]>>})
                        %combined = subop.combine_tuple %scan_matches, %t2
                        %gathered = subop.gather %combined @ngramlist::@lookup_ref { ngram_ht_1 => @ngramlist::@ngram({type= !db.string}),  id_ht_1 => @ngramlist::@id({type=i32}) }
                        %idsNotEq = subop.map %gathered computes : [@ngramlist::@idNotEq({type=i1})] (%tpl: !tuples.tuple){
                            %idxId = tuples.getcol %tpl @ngramlist::@id : i32
                            %neq = arith.cmpi ne, %currId, %idxId : i32
                            tuples.return %neq : i1
                        }
                        %filtered = subop.filter %idsNotEq all_true [@ngramlist::@idNotEq]
                        %lookedUp =subop.lookup_or_insert %filtered %cnt_map[@ngramlist::@id] : !subop.hashmap<[ id_ht2 : i32],[ cnt : i32]> @cntmap::@ref({type=!subop.lookup_entry_ref<!subop.hashmap<[ id_ht2 : i32],[ cnt : i32]>>})
                            eq: ([%l], [%r]){
                                %eq = arith.cmpi eq, %l, %r :i32
                                tuples.return %eq : i1
                            }
                            initial: {
                                %zero = arith.constant 0 : i32
                                tuples.return %zero : i32
                            }
                        %gatheredCnt = subop.gather %lookedUp @cntmap::@ref { cnt => @cntmap::@cnt({type=i32}) }
                        %incCnt = subop.map %gatheredCnt computes : [@cntmap::@newcnt({type=i32}), @cntmap::@emit({type=i1})] (%tpl: !tuples.tuple){
                            %currCount = tuples.getcol %tpl @cntmap::@cnt : i32
                            %c1 = arith.constant 1 : i32
                            %nextCount = arith.addi %currCount, %c1 : i32
                            %emitCount = arith.constant 10 : i32
                            %emit = arith.cmpi eq, %nextCount, %emitCount : i32
                            tuples.return %nextCount, %emit : i32, i1
                        }
                        subop.scatter %incCnt @cntmap::@ref { @cntmap::@newcnt => cnt }
                        %filteredWithCnt = subop.filter %incCnt all_true [@cntmap::@emit]
                        tuples.return %filteredWithCnt : !tuples.tuplestream
                    }
                    tuples.return %rel2_matches : !tuples.tuplestream
                }
                tuples.return %rel2_splitted: !tuples.tuplestream
            }
            %cp = relalg.crossproduct %nested, %t3
            %sel = relalg.selection %cp (%arg0: !tuples.tuple){
              %14 = tuples.getcol %arg0 @ngramlist::@id : i32
              %15 = tuples.getcol %arg0 @t2::@id : i32
              %16 = db.compare eq %14 : i32, %15 : i32
              tuples.return %16 :i1
            }
            %sel2 = relalg.selection %sel (%arg0: !tuples.tuple){
              %14 = tuples.getcol %arg0 @t1::@id : i32
              %15 = db.constant(100) :i32
              %16 = db.compare lt %14 : i32, %15 : i32
              tuples.return %16 :i1
            }


            %materialized = relalg.materialize %sel2 [@t1::@id,@t1::@title,@t2::@id, @t2::@title] => ["id","original","matching id","matching title"] : !subop.result_table<[id : i32,original: !db.string,matching_id: i32,matched_title:!db.string]>
            subop.set_result 0 %materialized :  !subop.result_table<[id : i32,original: !db.string,matching_id: i32,matched_title:!db.string]>
    return
  }
}

