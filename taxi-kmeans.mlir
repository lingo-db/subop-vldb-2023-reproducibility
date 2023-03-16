module {
  func.func @main() {
    %0 = relalg.basetable  {table_identifier = "taxi_rides"} columns: {d_lat => @taxi_rides::@d_lat({type = f64}), d_lon => @taxi_rides::@d_lon({type = f64}), d_t => @taxi_rides::@d_t({type = !db.timestamp<second>}), extra => @taxi_rides::@extra({type = !db.decimal<9, 2>}), fare_amount => @taxi_rides::@fare_amount({type = !db.decimal<9, 2>}), improvement_surcharge => @taxi_rides::@improvement_surcharge({type = !db.decimal<9, 2>}), mta_tax => @taxi_rides::@mta_tax({type = !db.decimal<9, 2>}), p_lat => @taxi_rides::@p_lat({type = f64}), p_lon => @taxi_rides::@p_lon({type = f64}), p_t => @taxi_rides::@p_t({type = !db.timestamp<second>}), passenger_count => @taxi_rides::@passenger_count({type = i32}), payment_type => @taxi_rides::@payment_type({type = i32}), ratecode_id => @taxi_rides::@ratecode_id({type = i32}), store_and_fwd_flag => @taxi_rides::@store_and_fwd_flag({type = !db.string}), tip_amount => @taxi_rides::@tip_amount({type = !db.decimal<9, 2>}), tolls_amount => @taxi_rides::@tolls_amount({type = !db.decimal<9, 2>}), total_amount => @taxi_rides::@total_amount({type = !db.decimal<9, 2>}), trip_distance => @taxi_rides::@trip_distance({type = f64}), vendor_id => @taxi_rides::@vendor_id({type = i32})}
    %1 = relalg.selection %0 (%arg0: !tuples.tuple){
      %5 = db.constant("second") : !db.char<6>
      %6 = tuples.getcol %arg0 @taxi_rides::@p_t : !db.timestamp<second>
      %7 = tuples.getcol %arg0 @taxi_rides::@d_t : !db.timestamp<second>
      %8 = db.runtime_call "DateDiff"(%5, %6, %7) : (!db.char<6>, !db.timestamp<second>, !db.timestamp<second>) -> i64
      %9 = db.constant(0 : i32) : i64
      %10 = db.compare gt %8 : i64, %9 : i64
      %11 = db.constant("hour") : !db.char<4>
      %12 = tuples.getcol %arg0 @taxi_rides::@p_t : !db.timestamp<second>
      %13 = db.runtime_call "ExtractFromDate"(%11, %12) : (!db.char<4>, !db.timestamp<second>) -> i64
      %14 = db.constant(20 : i32) : i64
      %15 = db.compare eq %13 : i64, %14 : i64
      %16 = tuples.getcol %arg0 @taxi_rides::@p_lon : f64
      %17 = db.constant(50 : i32) : f64
      %18 = db.compare lt %16 : f64, %17 : f64
      %19 = tuples.getcol %arg0 @taxi_rides::@p_lat : f64
      %20 = db.constant(30 : i32) : f64
      %21 = db.compare gt %19 : f64, %20 : f64
      %22 = db.and %10, %15, %18, %21 : i1, i1, i1, i1
      tuples.return %22 : i1
    }
    %2 = relalg.map %1 computes : [@map0::@tmp_attr0({type = i64})] (%arg0: !tuples.tuple){
      %5 = db.constant("second") : !db.char<6>
      %6 = tuples.getcol %arg0 @taxi_rides::@p_t : !db.timestamp<second>
      %7 = tuples.getcol %arg0 @taxi_rides::@d_t : !db.timestamp<second>
      %8 = db.runtime_call "DateDiff"(%5, %6, %7) : (!db.char<6>, !db.timestamp<second>, !db.timestamp<second>) -> i64
      tuples.return %8 : i64
    }
    %3 = relalg.map %2 computes : [@map1::@tmp_attr1({type = !db.decimal<28, 21>})] (%arg0: !tuples.tuple){
      %5 = tuples.getcol %arg0 @taxi_rides::@fare_amount : !db.decimal<9, 2>
      %6 = tuples.getcol %arg0 @map0::@tmp_attr0 : i64
      %7 = db.cast %6 : i64 -> !db.decimal<19, 0>
      %8 = db.div %5 : !db.decimal<9, 2>, %7 : !db.decimal<19, 0>
      tuples.return %8 : !db.decimal<28, 21>
    }

    %nested = relalg.nested %3 [@taxi_rides::@p_lon,@taxi_rides::@p_lat,@map1::@tmp_attr1] -> [@taxi_rides::@p_lon,@taxi_rides::@p_lat,@map1::@tmp_attr1,@nested::@clusterId] (%pointsStream){
        %initialCentroids = subop.create !subop.buffer<[initialClusterX : f64, initialClusterY : f64, initialClusterId : i32]>
        %ctr = subop.create_simple_state !subop.simple_state<[ctr:i32]> initial: {
             %c0 = db.constant(0) : i32
            tuples.return %c0 : i32
        }

        %numPoints = subop.create_simple_state !subop.simple_state<[numPoints: i64]> initial: {
            %c0 = db.constant(0) : i64
          tuples.return %c0 : i64
        }
        %points = subop.create !subop.buffer<[pointX : f64, pointY : f64, lucrativeness : !db.decimal<28, 21>]>
        subop.materialize %pointsStream {@taxi_rides::@p_lon =>pointX, @taxi_rides::@p_lat => pointY, @map1::@tmp_attr1 => lucrativeness}, %points: !subop.buffer<[pointX : f64, pointY : f64, lucrativeness : !db.decimal<28, 21>]>
        %numPointsRef = subop.lookup %pointsStream %numPoints[] : !subop.simple_state<[numPoints: i64]> @numPoints::@ref({type=!subop.lookup_entry_ref<!subop.simple_state<[numPoints: i64]>>})
        subop.reduce %numPointsRef @numPoints::@ref [] ["numPoints"] ([],[%currNumPoints]){
          %c1 = arith.constant 1 : i64
          %nextNumPoints = arith.addi %currNumPoints, %c1 : i64
          tuples.return %nextNumPoints : i64
        }
        %continuousPoints = subop.create_continuous_view %points : !subop.buffer<[pointX : f64, pointY : f64, lucrativeness : !db.decimal<28, 21>]> -> !subop.continuous_view<!subop.buffer<[pointX : f64, pointY : f64, lucrativeness : !db.decimal<28, 21>]>>
        %numPointsStream = subop.scan %numPoints : !subop.simple_state<[numPoints: i64]>  {numPoints => @numPoints::@value({type=i64})}

        %nested = subop.nested_map %numPointsStream [@numPoints::@value](%t, %n){
          %randomSampleStream = subop.generate [@generated::@id({type=i32}),@generated::@idx({type=index})] {
            %k = arith.constant 30 : index
            %c0 = arith.constant 0 : index
            %c064 = arith.constant 0 : i64
            %c1 = arith.constant 1 : index
            %false = arith.constant 0 : i1
            %previousIndices=util.alloca(%k) : !util.ref<index>
            scf.for %i = %c0 to %k step %c1 {
              %randomIdx = scf.while (%lastIdx = %c0) : (index) -> index {
                %currIdx64 = db.runtime_call "RandomInRange" (%c064, %n) : (i64,i64) -> (i64)
                %currIdx = arith.index_cast %currIdx64 : i64 to index
                %collision = scf.for %j = %c0 to %i step %c1 iter_args(%c = %false) -> (i1) {
                  %previousIdx=util.load %previousIndices[%j] :!util.ref<index> -> index
                  %eq = arith.cmpi eq, %currIdx, %previousIdx : index
                  %nC = arith.ori %eq, %c : i1 
                  scf.yield %nC : i1
                } 
                scf.condition(%collision) %currIdx : index
              } do {
              ^bb0(%arg1: index):
                scf.yield %arg1 : index
              }
              util.store %randomIdx : index, %previousIndices[%i] : !util.ref<index>
              %ias32 = arith.index_cast %i : index to i32
              subop.generate_emit %ias32, %randomIdx : i32, index
            }
            tuples.return
          }
          tuples.return %randomSampleStream : !tuples.tuplestream 
        }
        %beginRef = subop.get_begin_ref %nested %continuousPoints : !subop.continuous_view<!subop.buffer<[pointX : f64, pointY : f64, lucrativeness : !db.decimal<28, 21>]>> @view::@begin({type=!subop.continous_view_entry_ref<!subop.continuous_view<!subop.buffer<[pointX : f64, pointY : f64, lucrativeness : !db.decimal<28, 21>]>>>})
        %offsetRef = subop.offset_ref_by %beginRef @view::@begin @generated::@idx @view::@ref({type=!subop.continous_view_entry_ref<!subop.continuous_view<!subop.buffer<[pointX : f64, pointY : f64, lucrativeness : !db.decimal<28, 21>]>>>})
        %gathered = subop.gather %offsetRef @view::@ref { pointX => @sample::@x({type=f64}),pointY => @sample::@y({type=f64}) }
        subop.materialize %gathered {@sample::@x=>initialClusterX, @sample::@y => initialClusterY, @generated::@id => initialClusterId}, %initialCentroids: !subop.buffer<[initialClusterX : f64, initialClusterY : f64, initialClusterId : i32]>
        %timingStart = db.runtime_call "startTiming" () : () -> (i64)

        %finalCentroids = subop.loop %initialCentroids : !subop.buffer<[initialClusterX : f64, initialClusterY : f64, initialClusterId : i32]> (%centroids) -> !subop.buffer<[clusterX : f64, clusterY : f64, clusterId : i32]> {
                %nextCentroids = subop.create !subop.buffer<[nextClusterX : f64, nextClusterY : f64, nextClusterId : i32]>
                %hashmap = subop.create !subop.hashmap<[centroidId : i32],[sumX : f64, sumY : f64, count : i32]>
                %continuousCentroids = subop.create_continuous_view %centroids : !subop.buffer<[clusterX : f64, clusterY : f64, clusterId : i32]> -> !subop.continuous_view<!subop.buffer<[clusterX : f64, clusterY : f64, clusterId : i32]>>

                 %stream1 = subop.scan %points : !subop.buffer<[pointX : f64, pointY : f64, lucrativeness : !db.decimal<28, 21>]> {pointX => @point::@x({type=f64}),pointY => @point::@y({type=f64})}
                 %stream2 = subop.nested_map %stream1 [@point::@x,@point::@y](%t, %x, %y){
                      %local_best = subop.create_simple_state !subop.simple_state<[min_dist: f64, arg_min : i32]> initial: {
                         %initial_dist = db.constant(1000000000) : f64
                         %initial_id = db.constant(1000000000) : i32
                        tuples.return %initial_dist, %initial_id : f64,i32
                      }

                      %cstream = subop.scan %continuousCentroids : !subop.continuous_view<!subop.buffer<[clusterX : f64, clusterY : f64, clusterId : i32]>> {clusterX => @cluster::@x({type=f64}),clusterY => @cluster::@y({type=f64}),clusterId => @cluster::@id({type=i32})}
                      %cstream2 = subop.map %cstream computes : [@m::@dist({type=f64})] (%tpl: !tuples.tuple){
                         %clusterX = tuples.getcol %tpl @cluster::@x : f64
                         %clusterY = tuples.getcol %tpl @cluster::@y : f64
                         %diffX = arith.subf %clusterX, %x : f64
                         %diffY = arith.subf %clusterY, %y : f64
                         %diffX2 = arith.mulf %diffX, %diffX :f64
                         %diffY2 = arith.mulf %diffY, %diffY : f64
                         %dist = arith.addf %diffX2, %diffY2 : f64

                         tuples.return %dist : f64
                      }
                      %cstream3 = subop.lookup %cstream2 %local_best[] : !subop.simple_state<[min_dist: f64, arg_min : i32]> @local_best::@ref({type=!subop.lookup_entry_ref<!subop.simple_state<[min_dist: f64, arg_min : i32]>>})
                      subop.reduce %cstream3 @local_best::@ref [@m::@dist, @cluster::@id] ["min_dist","arg_min"] ([%curr_dist, %curr_id],[%min_dist,%min_id]){
                        %lt = arith.cmpf olt, %curr_dist, %min_dist : f64
                        %new_min_dist, %new_arg_min = scf.if %lt -> (f64,i32) {
                            scf.yield %curr_dist, %curr_id : f64, i32
                        } else {
                            scf.yield %min_dist, %min_id : f64 , i32
                        }
                        tuples.return %new_min_dist, %new_arg_min : f64, i32
                      }
                      %bstream = subop.scan %local_best : !subop.simple_state<[min_dist: f64, arg_min : i32]> {min_dist => @best::@dist({type=f64}),arg_min => @best::@id({type=i32})}
                    tuples.return %bstream : !tuples.tuplestream
                 }
                 %sstream3 =subop.lookup_or_insert %stream2 %hashmap[@best::@id] : !subop.hashmap<[centroidId : i32],[sumX : f64, sumY : f64, count : i32]> @aggr::@ref({type=!subop.lookup_entry_ref<!subop.hashmap<[centroidId : i32],[sumX : f64, sumY : f64, count : i32]>>})
                                        eq: ([%l], [%r]){
                                            %eq = arith.cmpi eq, %l, %r :i32
                                            tuples.return %eq : i1
                                        }
                                        initial: {
                                            %zero = arith.constant 0.0 : f64
                                            %zeroi = arith.constant 0 : i32
                                            tuples.return %zero,%zero,%zeroi : f64,f64, i32
                                        }
                  subop.reduce %sstream3 @aggr::@ref [@point::@x,@point::@y] ["sumX","sumY","count"] ([%curr_x, %curr_y],[%sum_x,%sum_y,%count]){
                    %c1 = arith.constant 1 : i32
                    %new_count = arith.addi %count, %c1 : i32
                    %new_sum_x = arith.addf %sum_x, %curr_x : f64
                    %new_sum_y = arith.addf %sum_y, %curr_y : f64
                    tuples.return %new_sum_x,%new_sum_y, %new_count : f64,f64,i32
                  } combine: ([%sumXa, %sumYa, %counta],[%sumXb, %sumYb, %countb]){
                        %c_count = arith.addi %counta, %countb : i32
                        %c_sum_x = arith.addf %sumXa, %sumXb : f64
                        %c_sum_y = arith.addf %sumYa, %sumYb : f64
                        tuples.return %c_sum_x,%c_sum_y,%c_count : f64,f64,i32
                  }
                 %fstream = subop.scan %hashmap : !subop.hashmap<[centroidId : i32],[sumX : f64, sumY : f64, count : i32]> {centroidId => @centroid::@id({type=i32}), sumX =>@hm::@sum_x({type=i32}) , sumY =>@hm::@sum_y({type=i32}),count => @hm::@count({type=i32})}
                 %fstream1 = subop.map %fstream computes : [@centroid::@x({type=f64}),@centroid::@y({type=f64})] (%tpl: !tuples.tuple){
                    %sum_x = tuples.getcol %tpl @hm::@sum_x : f64
                    %sum_y = tuples.getcol %tpl @hm::@sum_y : f64
                    %count = tuples.getcol %tpl @hm::@count : i32
                    %countf = arith.sitofp %count : i32 to f64
                    %x = arith.divf %sum_x, %countf : f64
                    %y = arith.divf %sum_y, %countf : f64
                    tuples.return %x, %y : f64, f64
                 }

                 subop.materialize %fstream1 {@centroid::@id => nextClusterId, @centroid::@x => nextClusterX, @centroid::@y => nextClusterY}, %nextCentroids : !subop.buffer<[nextClusterX : f64, nextClusterY : f64, nextClusterId : i32]>
                %changed = subop.create_simple_state !subop.simple_state<[changed :i1]> initial: {
                  %false = arith.constant 0 : i1
                  tuples.return %false : i1
                }
                 %cstream = subop.scan %centroids : !subop.buffer<[clusterX : f64, clusterY : f64, clusterId : i32]> {clusterX => @cluster::@x({type=f64}),clusterY => @cluster::@y({type=f64}),clusterId => @cluster::@id({type=i32})}
                 %cstream2 =subop.lookup_or_insert %cstream %hashmap[@cluster::@id] : !subop.hashmap<[centroidId : i32],[sumX : f64, sumY : f64, count : i32]> @hm::@ref({type=!subop.lookup_entry_ref<!subop.hashmap<[centroidId : i32],[sumX : f64, sumY : f64, count : i32]>>})
                                        eq: ([%l], [%r]){
                                            %eq = arith.cmpi eq, %l, %r :i32
                                            tuples.return %eq : i1
                                        } initial: {
                                             %zero = arith.constant 0.0 : f64
                                             %zeroi = arith.constant 0 : i32
                                             tuples.return %zero,%zero,%zeroi : f64,f64, i32
                                        }
                 %cstream3 = subop.lookup %cstream2 %changed[] :  !subop.simple_state<[changed :i1]> @changed::@ref({type=!subop.lookup_entry_ref< !subop.simple_state<[changed :i1]>>})
                 %cstream4 = subop.gather %cstream3 @hm::@ref {sumX => @hm::@sum_x({type=f64}), sumY => @hm::@sum_y({type=f64}), count => @hm::@count({type=i32})}
                  %cstream5 = subop.map %cstream4 computes : [@m::@iseq({type=i1})] (%tpl: !tuples.tuple){
                     %old_x = tuples.getcol %tpl @cluster::@x : f64
                     %old_y = tuples.getcol %tpl @cluster::@y : f64
                     %sum_x = tuples.getcol %tpl @hm::@sum_x : f64
                     %sum_y = tuples.getcol %tpl @hm::@sum_y : f64
                     %count = tuples.getcol %tpl @hm::@count : i32
                     %countf = arith.sitofp %count : i32 to f64
                     %x = arith.divf %sum_x, %countf : f64
                     %y = arith.divf %sum_y, %countf : f64
                     %xeq = arith.cmpf oeq, %x,%old_x : f64
                     %yeq = arith.cmpf oeq, %y,%old_y : f64
                     %botheq = arith.andi %xeq, %yeq : i1
                     tuples.return %botheq :i1
                  }
                  subop.reduce %cstream5 @changed::@ref [@m::@iseq] ["changed"] ([%iseq],[%has_changed]){
                    %c1 = arith.constant 1 : i1
                    %notEq = arith.xori %iseq,%c1 :i1
                    %new_has_changed = arith.ori %notEq, %has_changed : i1
                    tuples.return %new_has_changed : i1
                  }
                  %changed_stream = subop.scan %changed  :  !subop.simple_state<[changed :i1]> {changed => @s::@changed({type=i1})}
                  %ctrLookup = subop.lookup %changed_stream %ctr[] : !subop.simple_state<[ctr:i32]> @s::@ref({type=!subop.entry_ref<!subop.simple_state<[ctr:i32]>>})

                  %21 = subop.gather %ctrLookup @s::@ref {ctr=> @s::@ctr({type=i32})}
                  %s23 = subop.map %21 computes: [@m::@p1({type=i32}),@m::@continue({type=i1})] (%tpl: !tuples.tuple){
                      %ctrVal = tuples.getcol %tpl @s::@ctr : i32
                      %hasChanged = tuples.getcol %tpl @s::@changed: i1

                      %c1 = db.constant(1) : i32
                      %p1 = arith.addi %c1, %ctrVal : i32
                      %c5 = arith.constant 50 : i32
                      %p1Lt5 = arith.cmpi slt, %p1, %c5 : i32
                      %continue = arith.andi %hasChanged, %p1Lt5 : i1
                      tuples.return %p1, %continue : i32,i1
                  }
                  subop.scatter %s23 @s::@ref {@m::@p1 => ctr}
                 subop.loop_continue (%s23 [@m::@continue]) %nextCentroids : !subop.buffer<[nextClusterX : f64, nextClusterY : f64, nextClusterId : i32]>
        }
                 db.runtime_call "stopTiming" (%timingStart) : (i64) -> ()
        %fstream1 = subop.scan %points : !subop.buffer<[pointX : f64, pointY : f64, lucrativeness : !db.decimal<28, 21>]> {pointX => @taxi_rides::@p_lon({type=f64}),pointY => @taxi_rides::@p_lat({type=f64}), lucrativeness => @map1::@tmp_attr1({type=!db.decimal<28, 21>})}
        %fstream2 = subop.nested_map %fstream1 [@taxi_rides::@p_lon,@taxi_rides::@p_lat](%t, %x, %y){
            %local_best = subop.create_simple_state !subop.simple_state<[min_dist_final: f64, arg_min_final : i32]> initial: {
                %initial_dist = db.constant(1000000000) : f64
                %initial_id = db.constant(1000000000) : i32
              tuples.return %initial_dist, %initial_id : f64,i32
            }
            %cstream = subop.scan %finalCentroids : !subop.buffer<[clusterX : f64, clusterY : f64, clusterId : i32]> {clusterX => @cluster::@x({type=f64}),clusterY => @cluster::@y({type=f64}),clusterId => @cluster::@id({type=i32})}
            %cstream2 = subop.map %cstream computes : [@m::@dist({type=f64})] (%tpl: !tuples.tuple){
                %clusterX = tuples.getcol %tpl @cluster::@x : f64
                %clusterY = tuples.getcol %tpl @cluster::@y : f64
                %diffX = arith.subf %clusterX, %x : f64
                %diffY = arith.subf %clusterY, %y : f64
                %diffX2 = arith.mulf %diffX, %diffX :f64
                %diffY2 = arith.mulf %diffY, %diffY : f64
                %dist = arith.addf %diffX2, %diffY2 : f64

                tuples.return %dist : f64
            }
            %cstream3 = subop.lookup %cstream2 %local_best[] : !subop.simple_state<[min_dist_final: f64, arg_min_final : i32]> @local_best_final::@ref({type=!subop.lookup_entry_ref<!subop.simple_state<[min_dist_final: f64, arg_min_final : i32]>>})
            subop.reduce %cstream3 @local_best_final::@ref [@m::@dist, @cluster::@id] ["min_dist_final","arg_min_final"] ([%curr_dist, %curr_id],[%min_dist,%min_id]){
              %lt = arith.cmpf olt, %curr_dist, %min_dist : f64
              %new_min_dist, %new_arg_min = scf.if %lt -> (f64,i32) {
                  scf.yield %curr_dist, %curr_id : f64, i32
              } else {
                  scf.yield %min_dist, %min_id : f64 , i32
              }
              tuples.return %new_min_dist, %new_arg_min : f64, i32
            }
          %bstream = subop.scan %local_best : !subop.simple_state<[min_dist_final: f64, arg_min_final : i32]> {min_dist_final => @best::@dist({type=f64}),arg_min_final => @nested::@clusterId({type=i32})}
          %combined = subop.combine_tuple %bstream, %t
          tuples.return %combined : !tuples.tuplestream
        }
        tuples.return %fstream2 : !tuples.tuplestream
    }
    %aggr = relalg.aggregation %nested [@nested::@clusterId] computes : [@aggr::@avgLucrativness({type=!db.decimal<38, 31>}),@aggr::@minLon({type=f64}),@aggr::@maxLon({type=f64}),@aggr::@minLat({type=f64}),@aggr::@maxLat({type=f64})] (%arg0: !tuples.tuplestream, %arg1: !tuples.tuple) {
      %avg = relalg.aggrfn avg @map1::@tmp_attr1 %arg0 : !db.decimal<38, 31>
      %minLon = relalg.aggrfn min @taxi_rides::@p_lon %arg0 : f64
      %maxLon = relalg.aggrfn max @taxi_rides::@p_lon %arg0 : f64
      %minLat = relalg.aggrfn min @taxi_rides::@p_lat %arg0 : f64
      %maxLat = relalg.aggrfn max @taxi_rides::@p_lat %arg0 : f64

      tuples.return %avg, %minLon, %maxLon, %minLat, %maxLat :  !db.decimal<38, 31>, f64, f64, f64, f64
    }
    %4 = relalg.materialize %aggr [@nested::@clusterId,@aggr::@avgLucrativness,@aggr::@minLon,@aggr::@maxLon,@aggr::@minLat,@aggr::@maxLat] => ["cluster","lucrativness","minLon","maxLon","minLat","maxLat"] : !subop.result_table<[clusterRes$0 : i32, avgLucrativness$0 : !db.decimal<38, 31>, minLon$0 : f64, maxLon$0 : f64, minLat$0 : f64, maxLat$0 : f64]>
    subop.set_result 0 %4 : !subop.result_table<[clusterRes$0 : i32, avgLucrativness$0 : !db.decimal<38, 31>, minLon$0 : f64, maxLon$0 : f64, minLat$0 : f64, maxLat$0 : f64]>
    return
  }
}