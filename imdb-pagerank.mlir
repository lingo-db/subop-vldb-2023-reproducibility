//RUN: run-mlir %s %S/../../../resources/data/job | FileCheck %s

module {
  func.func @main() {
    %0 = relalg.basetable  {table_identifier = "cast_info"} columns: {id => @c1_::@id({type = i32}), movie_id => @c1_::@movie_id({type = i32}), note => @c1_::@note({type = !db.nullable<!db.string>}), nr_order => @c1_::@nr_order({type = !db.nullable<i32>}), person_id => @c1_::@person_id({type = i32}), person_role_id => @c1_::@person_role_id({type = !db.nullable<i32>}), role_id => @c1_::@role_id({type = i32})}
    %1 = relalg.basetable  {table_identifier = "cast_info"} columns: {id => @c2_::@id({type = i32}), movie_id => @c2_::@movie_id({type = i32}), note => @c2_::@note({type = !db.nullable<!db.string>}), nr_order => @c2_::@nr_order({type = !db.nullable<i32>}), person_id => @c2_::@person_id({type = i32}), person_role_id => @c2_::@person_role_id({type = !db.nullable<i32>}), role_id => @c2_::@role_id({type = i32})}
    %2 = relalg.crossproduct %0, %1
    %3 = relalg.basetable  {table_identifier = "role_type"} columns: {id => @r::@id({type = i32}), role => @r::@role({type = !db.string})}
    %4 = relalg.crossproduct %2, %3
    %5 = relalg.basetable  {table_identifier = "movie_info"} columns: {id => @mi::@id({type = i32}), info => @mi::@info({type = !db.string}), info_type_id => @mi::@info_type_id({type = i32}), movie_id => @mi::@movie_id({type = i32}), note => @mi::@note({type = !db.nullable<!db.string>})}
    %6 = relalg.crossproduct %4, %5
    %7 = relalg.basetable  {table_identifier = "info_type"} columns: {id => @it::@id({type = i32}), info => @it::@info({type = !db.string})}
    %8 = relalg.crossproduct %6, %7
    %9 = relalg.selection %8 (%arg0: !tuples.tuple){
      %11 = tuples.getcol %arg0 @c1_::@movie_id : i32
      %12 = tuples.getcol %arg0 @c2_::@movie_id : i32
      %13 = db.compare eq %11 : i32, %12 : i32
      %14 = tuples.getcol %arg0 @c1_::@role_id : i32
      %15 = tuples.getcol %arg0 @r::@id : i32
      %16 = db.compare eq %14 : i32, %15 : i32
      %17 = tuples.getcol %arg0 @c2_::@role_id : i32
      %18 = tuples.getcol %arg0 @r::@id : i32
      %19 = db.compare eq %17 : i32, %18 : i32
      %20 = tuples.getcol %arg0 @r::@role : !db.string
      %21 = db.constant("actor") : !db.string
      %22 = db.compare eq %20 : !db.string, %21 : !db.string
      %23 = tuples.getcol %arg0 @c1_::@nr_order : !db.nullable<i32>
      %24 = tuples.getcol %arg0 @c2_::@nr_order : !db.nullable<i32>
      %25 = db.compare gt %23 : !db.nullable<i32>, %24 : !db.nullable<i32>
      %26 = tuples.getcol %arg0 @it::@id : i32
      %27 = tuples.getcol %arg0 @mi::@info_type_id : i32
      %28 = db.compare eq %26 : i32, %27 : i32
      %29 = tuples.getcol %arg0 @it::@info : !db.string
      %30 = db.constant("genres") : !db.string
      %31 = db.compare eq %29 : !db.string, %30 : !db.string
      %32 = tuples.getcol %arg0 @mi::@info : !db.string
      %33 = db.constant("Drama") : !db.string
      %34 = db.compare eq %32 : !db.string, %33 : !db.string
      %35 = tuples.getcol %arg0 @mi::@movie_id : i32
      %36 = tuples.getcol %arg0 @c1_::@movie_id : i32
      %37 = db.compare eq %35 : i32, %36 : i32
      %38 = db.and %13, %16, %19, %22, %25, %28, %31, %34, %37 : i1, i1, i1, i1, !db.nullable<i1>, i1, i1, i1, i1
      tuples.return %38 : !db.nullable<i1>
    }
    %nested = relalg.nested %9 [@c1_::@person_id,@c2_::@person_id] -> [@nested::@person_id,@nested::@rank] (%edgeData){
        %numVertices = subop.create_simple_state !subop.simple_state<[numVertices: index]> initial: {
             %c0 = arith.constant 0  : index
            tuples.return %c0 : index
        }
        %rawEdges = subop.create !subop.buffer<[edgeFromRaw : i32, edgeToRaw : i32]>
         subop.materialize %edgeData {@c1_::@person_id=>edgeFromRaw, @c2_::@person_id => edgeToRaw}, %rawEdges : !subop.buffer<[edgeFromRaw : i32, edgeToRaw : i32]>


        %edges = subop.create !subop.buffer<[edgeFrom : i32, edgeTo : i32]>
        %vertexMapping = subop.create !subop.hashmap<[ vertexId : i32],[denseId : i32]>
        %reverseVertexMapping = subop.create !subop.hashmap<[ revDenseId : i32],[revVertexId : i32]>
        %bufferedEdgeData=subop.scan %rawEdges : !subop.buffer<[edgeFromRaw : i32, edgeToRaw : i32]> {edgeFromRaw=>@c1_::@person_id({type=i32}),edgeToRaw=>@c2_::@person_id({type=i32})}
        %lookedMappingUpFrom = subop.lookup_or_insert %bufferedEdgeData %vertexMapping[@c1_::@person_id] : !subop.hashmap<[ vertexId : i32],[denseId : i32]> @fromMapping::@ref({type=!subop.lookup_entry_ref<!subop.hashmap<[ vertexId : i32],[denseId : i32]>>})
            eq: ([%l], [%r]){
                %eq = arith.cmpi eq, %l, %r :i32
                tuples.return %eq : i1
            }
            initial: {
                %m1 = arith.constant -1 : i32
                tuples.return %m1 : i32
            }
        %lookedMappingUpTo = subop.lookup_or_insert %lookedMappingUpFrom %vertexMapping[@c2_::@person_id] : !subop.hashmap<[ vertexId : i32],[denseId : i32]> @toMapping::@ref({type=!subop.lookup_entry_ref<!subop.hashmap<[ vertexId : i32],[denseId : i32]>>})
            eq: ([%l], [%r]){
                %eq = arith.cmpi eq, %l, %r :i32
                tuples.return %eq : i1
            }
            initial: {
                %m1 = arith.constant -1 : i32
                tuples.return %m1 : i32
            }
        %gatheredFromDenseId = subop.gather %lookedMappingUpTo @fromMapping::@ref {denseId => @from::@denseId({type=i32})}
        %gatheredToDenseId = subop.gather %gatheredFromDenseId @toMapping::@ref {denseId => @to::@denseId({type=i32})}
        %lookedUpNV = subop.lookup %gatheredToDenseId %numVertices[] : !subop.simple_state<[numVertices:index]> @numVertices::@ref({type=!subop.entry_ref<!subop.simple_state<[numVertices:index]>>})
        %gatheredNV= subop.gather %lookedUpNV @numVertices::@ref {numVertices => @numVertices::@val({type=index})}
        %newDenseIds = subop.map %gatheredNV computes: [@m::@newFromId({type=i32}),@m::@newToId({type=i32}), @m::@newNumElements({type=index})] (%tpl: !tuples.tuple){
             %numV = tuples.getcol %tpl @numVertices::@val : index
             %from = tuples.getcol %tpl @c1_::@person_id : i32
             %to = tuples.getcol %tpl @c2_::@person_id : i32
             %fromDense = tuples.getcol %tpl @from::@denseId : i32
             %toDense = tuples.getcol %tpl @to::@denseId : i32
             %c0 = arith.constant 0 : i32
             %c1i = arith.constant 1 : index
             %fromInvalid = arith.cmpi slt, %fromDense,%c0 : i32
             %toInvalid = arith.cmpi slt, %toDense,%c0 : i32
             %newFromId, %numVertices1 = scf.if %fromInvalid -> (i32, index) {
                %newFromDense = arith.index_cast %numV : index to i32
                %newNumVertices = arith.addi %numV, %c1i : index
                scf.yield %newFromDense, %newNumVertices : i32, index
             } else {
                scf.yield %fromDense, %numV : i32, index
             }
              %newToId, %numVertices2 = scf.if %toInvalid -> (i32, index) {
                 %newToDense = arith.index_cast %numVertices1 : index to i32
                 %newNumVertices = arith.addi %numVertices1, %c1i : index
                 scf.yield %newToDense, %newNumVertices : i32, index
              } else {
                 scf.yield %toDense, %numVertices1 : i32, index
              }
             tuples.return %newFromId, %newToId, %numVertices2 : i32, i32, index
        }
        subop.materialize %newDenseIds {@m::@newFromId=>edgeFrom, @m::@newToId => edgeTo}, %edges : !subop.buffer<[edgeFrom : i32, edgeTo : i32]>
        subop.scatter %newDenseIds @fromMapping::@ref {@m::@newFromId => denseId}
        subop.scatter %newDenseIds @toMapping::@ref {@m::@newToId => denseId}
        subop.scatter %newDenseIds @numVertices::@ref { @m::@newNumElements => numVertices}

        %rStream1 = subop.scan %vertexMapping : !subop.hashmap<[ vertexId : i32],[denseId : i32]> {vertexId => @vM::@vertexId({type=i32}), denseId  => @vM::@denseId({type=i32})}
        
        %rStream2 = subop.lookup_or_insert %rStream1 %reverseVertexMapping[@vM::@denseId] : !subop.hashmap<[ revDenseId : i32],[revVertexId : i32]> @rM::@ref({type=!subop.lookup_entry_ref<!subop.hashmap<[ revDenseId : i32],[revVertexId : i32]>>})
        eq: ([%l], [%r]){
            %eq = arith.cmpi eq, %l, %r :i32
            tuples.return %eq : i1
        }
        initial: {
            %c0 = arith.constant 0 : i32
            tuples.return %c0 : i32
        }
        subop.scatter %rStream2 @rM::@ref {@vM::@vertexId => revVertexId}
        
        %initialWeights = subop.create_array %numVertices : !subop.simple_state<[numVertices:index]> -> !subop.array<[initialRank : f64, initialL: i32]>
        %iStream1 = subop.scan %edges :!subop.buffer<[edgeFrom : i32, edgeTo : i32]> {edgeFrom => @edge::@from1({type=i32}),edgeTo => @edge::@to1({type=i32})}
        %iStream2 = subop.get_begin_ref %iStream1 %initialWeights :!subop.array<[initialRank : f64, initialL: i32]> @view::@begin({type=!subop.continous_entry_ref<!subop.array<[initialRank : f64, initialL: i32]>>})
        %iStream3 = subop.offset_ref_by %iStream2 @view::@begin @edge::@from1 @view::@ref({type=!subop.continous_entry_ref<!subop.continuous_view<!subop.array<[initialRank : f64, initialL: i32]>>>})
        %iStream4 = subop.lookup %iStream3 %numVertices[] : !subop.simple_state<[numVertices:index]> @numVertices::@ref2({type=!subop.entry_ref<!subop.simple_state<[numVertices:index]>>})
        %iStream5 = subop.gather %iStream4 @numVertices::@ref2 {numVertices => @numVertices::@val2({type=index})}
       subop.reduce %iStream5 @view::@ref [@numVertices::@val2] ["initialRank","initialL"] ([%totalVertices],[%currRank, %currL]){
            %c1 = arith.constant 1 : i32
            %newL = arith.addi %currL, %c1 : i32
            %c1f = arith.constant 1.0 : f64
            %totalVerticesI64 = arith.index_cast %totalVertices : index to i64
            %totalVerticesf = arith.uitofp %totalVerticesI64 : i64 to f64
            %newRank = arith.divf %c1f, %totalVerticesf : f64

            tuples.return %newRank, %newL : f64, i32
        }
        %ctr = subop.create_simple_state !subop.simple_state<[ctr:i32]> initial: {
             %c0 = db.constant(0) : i32
            tuples.return %c0 : i32
        }
        %timingStart = db.runtime_call "startTiming" () : () -> (i64)
        db.runtime_call "startPerf" () : () -> ()
        %finalWeights = subop.loop %initialWeights :  !subop.array<[initialRank : f64, initialL: i32]> (%weights) ->  !subop.array<[rank : f64, l: i32]> {
                %nextWeights = subop.create_array %numVertices : !subop.simple_state<[numVertices:index]> -> !subop.array<[nextRank: f64,nextL : i32]>
                %iLStream1 = subop.scan_refs %nextWeights : !subop.array<[nextRank: f64,nextL : i32]> @nextWeightsView::@ref({type=!subop.continous_entry_ref<!subop.array<[nextRank: f64,nextL : i32]>>})
                %iLStream2 = subop.lookup %iLStream1 %numVertices[] : !subop.simple_state<[numVertices:index]> @numVertices::@ref3({type=!subop.entry_ref<!subop.simple_state<[numVertices:index]>>})
                %iLStream3 = subop.gather %iLStream2 @numVertices::@ref3 {numVertices => @numVertices::@val3({type=index})}
                %iLStream4 = subop.map %iLStream3 computes: [@m::@initialRank({type=f64})] (%tpl: !tuples.tuple){
                    %totalVertices = tuples.getcol %tpl @numVertices::@val3 : index
                    %totalVerticesI64 = arith.index_cast %totalVertices : index to i64
                    %totalVerticesf = arith.uitofp %totalVerticesI64 : i64 to f64
                    %c15 = arith.constant 0.15 : f64
                    %initialRank = arith.divf %c15,%totalVerticesf : f64
                    tuples.return %initialRank : f64
                }
                %iLStream5 = subop.get_begin_ref %iLStream4 %nextWeights :!subop.array<[nextRank : f64, nextL: i32]> @nextWeightsView::@begin({type=!subop.continous_entry_ref<!subop.array<[nextRank : f64, nextL: i32]>>})
                %iLStream6 =  subop.entries_between %iLStream5 @nextWeightsView::@begin @nextWeightsView::@ref @nextWeightsView::@id({type=index})
                %iLStream7 = subop.get_begin_ref %iLStream6 %weights :!subop.array<[rank : f64, l: i32]> @weightsView::@begin({type=!subop.continous_entry_ref<!subop.array<[rank : f64, l: i32]>>})
                %iLStream8 = subop.offset_ref_by %iLStream7 @weightsView::@begin @nextWeightsView::@id @weights::@ref({type=!subop.continous_entry_ref<!subop.array<[rank : f64, l: i32]>>})
                %iLStream9 = subop.gather %iLStream8 @weights::@ref {l => @weights::@l({type=i32})}
                subop.scatter %iLStream9 @nextWeightsView::@ref { @m::@initialRank => nextRank, @weights::@l => nextL }
                %stream1 = subop.scan %edges :!subop.buffer<[edgeFrom : i32, edgeTo : i32]> {edgeFrom => @edge::@from({type=i32}),edgeTo => @edge::@to({type=i32})} {attr="1"}
                %stream2 = subop.get_begin_ref %stream1 %weights :!subop.array<[rank : f64, l: i32]> @weightsView::@begin({type=!subop.continous_entry_ref<!subop.array<[rank : f64, l: i32]>>})
                %stream3 = subop.offset_ref_by %stream2 @weightsView::@begin @edge::@from @from::@ref({type=!subop.continous_entry_ref<!subop.array<[rank : f64, l: i32]>>})
                %stream4 = subop.get_begin_ref %stream3 %nextWeights :!subop.array<[nextRank: f64,nextL : i32]> @nextWeightsView::@begin({type=!subop.continous_entry_ref<!subop.array<[nextRank: f64,nextL : i32]>>})
                %stream5 = subop.offset_ref_by %stream4 @nextWeightsView::@begin @edge::@to @to::@ref({type=!subop.continous_entry_ref<!subop.array<[nextRank: f64,nextL : i32]>>})
                %gatheredFrom = subop.gather %stream5 @from::@ref {rank => @from::@rank({type=f64}), l => @from::@l({type=i32})}
                subop.reduce %gatheredFrom @to::@ref [@from::@rank,@from::@l] ["nextRank"] ([%currRank,%currL],[%rank]){
                    %c085 = arith.constant 0.85 : f64
                    %c1 = arith.constant 1 : i32
                    %safeL = arith.maxui %c1, %currL :i32
                    %currLF= arith.uitofp %safeL : i32 to f64
                    %toAdd = arith.divf %currRank, %currLF : f64
                    %damped= arith.mulf %toAdd, %c085 : f64
                    %newRank = arith.addf %rank, %damped : f64
                    tuples.return %newRank: f64
                }

            %20 = subop.scan_refs %ctr : !subop.simple_state<[ctr:i32]> @s::@ref({type=!subop.entry_ref<!subop.simple_state<[ctr:i32]>>})
            %21 = subop.gather %20 @s::@ref {ctr=> @s::@ctr({type=i32})}
            %s23 = subop.map %21 computes: [@m::@p1({type=i32}),@m::@continue({type=i1})] (%tpl: !tuples.tuple){
                 %ctrVal = tuples.getcol %tpl @s::@ctr : i32
                 %c1 = db.constant(1) : i32
                 %p1 = arith.addi %c1, %ctrVal : i32
                 %c5 = arith.constant 100 : i32
                 %p1Lt5 = arith.cmpi slt, %p1, %c5 : i32
                 tuples.return %p1, %p1Lt5 : i32,i1
            }
            subop.scatter %s23 @s::@ref {@m::@p1 => ctr}
            subop.loop_continue (%s23[@m::@continue]) %nextWeights :!subop.array<[nextRank: f64,nextL : i32]>
        }
         db.runtime_call "stopPerf" () : () -> ()
         db.runtime_call "stopTiming" (%timingStart) : (i64) -> ()
         %fstream1 = subop.scan_refs %finalWeights : !subop.array<[rank : f64, l: i32]> @finalWeights::@ref({type=!subop.continous_entry_ref<!subop.array<[rank : f64, l: i32]>>})
         %fstream2 = subop.gather %fstream1 @finalWeights::@ref { rank => @nested::@rank({type=f64})}
         %fstream3 = subop.get_begin_ref %fstream2 %finalWeights : !subop.array<[rank : f64, l: i32]> @finalWeights::@begin({type=!subop.continous_entry_ref<!subop.array<[rank : f64, l: i32]>>})
         %fstream4 =  subop.entries_between %fstream3 @finalWeights::@begin @finalWeights::@ref @finalWeights::@id({type=i32})
         %fstream5 = subop.lookup_or_insert %fstream4 %reverseVertexMapping[@finalWeights::@id] : !subop.hashmap<[ revDenseId : i32],[revVertexId : i32]> @rM::@ref({type=!subop.lookup_entry_ref<!subop.hashmap<[ revDenseId : i32],[revVertexId : i32]>>})
            eq: ([%l], [%r]){
                %eq = arith.cmpi eq, %l, %r :i32
                tuples.return %eq : i1
            }
            initial: {
                %c0 = arith.constant 0 : i32
                tuples.return %c0 : i32
            }
         %fstream6 = subop.gather %fstream5 @rM::@ref {revVertexId => @nested::@person_id({type=i32})}
         tuples.return %fstream6 : !tuples.tuplestream
    }
    %aka_name = relalg.basetable  {table_identifier = "aka_name"} columns: {id => @n::@id({type = i32}), imdb_index => @n::@imdb_index({type = !db.nullable<!db.string>}), md5sum => @n::@md5sum({type = !db.nullable<!db.string>}), name => @n::@name({type = !db.string}), name_pcode_cf => @n::@name_pcode_cf({type = !db.nullable<!db.string>}), name_pcode_nf => @n::@name_pcode_nf({type = !db.nullable<!db.string>}), person_id => @n::@person_id({type = i32}), surname_pcode => @n::@surname_pcode({type = !db.nullable<!db.string>})}
    %cp1 = relalg.crossproduct %aka_name, %nested
    %sel = relalg.selection %cp1 (%arg0: !tuples.tuple){
      %s1 = tuples.getcol %arg0 @n::@person_id : i32
      %s2 = tuples.getcol %arg0 @nested::@person_id : i32
      %s3 = db.compare eq %s1 : i32, %s2 : i32
      tuples.return %s3 : i1
    }
    %aggr = relalg.aggregation %sel [@n::@person_id] computes : [@aggr0::@tmp_attr1({type = f64}),@aggr0::@tmp_attr0({type = !db.string})] (%arg0: !tuples.tuplestream,%arg1: !tuples.tuple){
      %a1 = relalg.aggrfn min @nested::@rank %arg0 : f64
      %a2 = relalg.aggrfn max @n::@name %arg0 : !db.string
      tuples.return %a1, %a2 : f64, !db.string
    }
    %s = relalg.sort %aggr [(@aggr0::@tmp_attr1,desc)]
    %l = relalg.limit 10 %s
    %m = relalg.materialize %l [@n::@person_id,@aggr0::@tmp_attr0,@aggr0::@tmp_attr1] => ["person_id", "min", "pagerank"] : !subop.result_table<[person_id$0 : i32, min$0 : !db.string, pagerank$0 : f64]>
    subop.set_result 0 %m : !subop.result_table<[person_id$0 : i32, min$0 : !db.string, pagerank$0 : f64]>
    return
  }
}