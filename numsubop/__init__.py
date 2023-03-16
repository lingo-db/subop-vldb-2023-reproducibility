class Builder:
    def __init__(self):
        self.mlir = ""
        self.x_ctr = 0
        self.var_ctr = 0
        self.stream_ctr = 0
        self.ref_ctr = 0

    def get_stream_name(self):
        res = self.stream_ctr
        self.stream_ctr += 1
        return "stream" + str(res)

    def get_ref_col(self):
        res = self.ref_ctr
        self.ref_ctr += 1
        return "@ref" + str(res) + "::@ref"

    def create_size(self, size):
        varname = "var" + str(self.var_ctr)
        self.var_ctr += 1
        self.mlir += '''
        %{varname} = subop.create_simple_state !subop.simple_state<[numElements: index]> initial: {{
             %c10 = arith.constant {size}  : index
            tuples.return %c10 : index
        }}
        '''.format(**{"size": size, "varname": varname})
        return varname

    def start_timing(self):
        self.mlir += "%timingStart = db.runtime_call \"startTiming\" () : () -> (i64)\n"

    def stop_timing(self):
        self.mlir += "db.runtime_call \"stopTiming\" (%timingStart) : (i64) -> ()\n"

    def fill(self, array, size, const):

        self.mlir += '''
        %{generated} = subop.generate [@generate{xname}::@i({{type=index}}),@generate{xname}::@v({{type=f64}})] {{
            %n = arith.constant {size} : index
            %c0 = arith.constant 0 : index
            %c1 = arith.constant 1 : index
            scf.for %i = %c0 to %n step %c1 {{
                %const=arith.constant {const} : f64
                subop.generate_emit %i, %const : index, f64
            }}
            tuples.return
        }}
        %{stream1} = subop.get_begin_ref %{generated} %{varname} : !subop.array<[{xname} : f64]> @generate{xname}::@begin({{type=!subop.continous_entry_ref<!subop.array<[{xname} : f64]>>}})
        %{stream2} = subop.offset_ref_by %{stream1} @generate{xname}::@begin @generate{xname}::@i @generate{xname}::@ref({{type=!subop.continous_entry_ref<!subop.array<[{xname} : f64]>>}})
        subop.scatter %{stream2} @generate{xname}::@ref {{@generate{xname}::@v => {xname}}}
        '''.format(**{"size": size, "varname": array.varname, "xname": array.xname, "stream1": self.get_stream_name(),
                      "generated": self.get_stream_name(),
                      "stream2": self.get_stream_name(), "const": str(float(const))})

    def create_array(self, xname, size):
        varname = "var" + str(self.var_ctr)
        self.var_ctr += 1
        self.mlir += "%" + varname + " = subop.create_array %" + size + " : !subop.simple_state<[numElements: index]> -> !subop.array<[" + xname + " : f64]>\n"
        return varname

    def load_other(self, other, streamname, offsetcol):
        gathername = self.get_stream_name()
        othercolname = "@col::@" + other.xname
        res = '''
        %{startname} = subop.get_begin_ref %{streamname} %{other} : !subop.array<[{otherxname} : f64]> {startref}({{type=!subop.continous_entry_ref<!subop.array<[{otherxname} : f64]>>}})
        %{iname} = subop.offset_ref_by %{startname} {startref} {icol}  {otherref}({{type=!subop.continous_entry_ref<!subop.array<[{otherxname} : f64]>>}})
        %{gathername} = subop.gather %{iname}{otherref} {{{otherxname}=>{othercolname}({{type=f64}})}}
         '''.format(
            **{
                "gathername": gathername,
                "startname": self.get_stream_name(),
                "iname": self.get_stream_name(),
                "other": other.varname,
                "startref": self.get_ref_col(),
                "otherref": self.get_ref_col(),

                "icol": offsetcol,
                "streamname": streamname,
                "otherxname": other.xname,
                "othercolname": othercolname
            })
        return res, gathername, othercolname

    def write_updated_to(self, current, target, mapfn, other=None):
        gathername = self.get_stream_name()
        icol = self.get_ref_col()
        if other is not None:
            loadOtherCode, toMapName, otherCol = self.load_other(other, gathername, icol)
        else:
            loadOtherCode = ""
            toMapName = gathername
            otherCol = None
        self.mlir += '''
%{scanname} = subop.scan_refs %{srcname} : !subop.array<[{srcxname} : f64]> {ref1}({{type=!subop.continous_entry_ref<!subop.array<[{srcxname} : f64]>>}})
%{startname} = subop.get_begin_ref %{scanname} %{srcname} : !subop.array<[{srcxname} : f64]> {startref}({{type=!subop.continous_entry_ref<!subop.array<[{srcxname} : f64]>>}})
%{iname} = subop.entries_between %{startname} {startref} {ref1} {icol}({{type=index}})
%{gathername} = subop.gather %{iname} {ref1} {{{srcxname} => {xcolname}({{type=f64}})}}
{loadOther}
%{mapname} = subop.map %{toMapName} computes: [{updatedxcolname}({{type=f64}})] (%tpl : !tuples.tuple){{
    %current_x= tuples.getcol %tpl {xcolname} : f64
    {getOtherCol}

    {mapfn}
}}

%{otherstartname} = subop.get_begin_ref %{mapname} %{dstname} : !subop.array<[{dstxname} : f64]> {otherstartref}({{type=!subop.continous_entry_ref<!subop.array<[{dstxname} : f64]>>}})
%{otheriname} = subop.offset_ref_by %{otherstartname}  {otherstartref} {icol} {otherref}({{type=!subop.continous_entry_ref<!subop.array<[{dstxname} : f64]>>}})
subop.scatter %{otheriname} {otherref} {{{updatedxcolname}=>{dstxname}}}


 '''.format(
            **{"scanname": self.get_stream_name(), "mapname": self.get_stream_name(),
               "gathername": gathername,
               "startname": self.get_stream_name(),
               "otherstartname": self.get_stream_name(),
               "iname": self.get_stream_name(),
               "otheriname": self.get_stream_name(),
               "toMapName": toMapName,
               "srcname": current.varname,
               "dstname": target.varname,
               "srcxname": current.xname,
               "dstxname": target.xname,
               "xcolname": "@col::@" + current.xname,
               "updatedxcolname": "@updated::@" + current.xname,
               "ref1": self.get_ref_col(),
               "startref": self.get_ref_col(),
               "otherstartref": self.get_ref_col(),
               "otherref": self.get_ref_col(),
               "icol": icol,

               "loadOther": loadOtherCode,
               "getOtherCol": "" if otherCol is None else "%other_x=tuples.getcol %tpl " + otherCol + " : f64",
               "mapfn": mapfn("current_x") if otherCol is None else mapfn("current_x", "other_x")})

    def return_table(self, array, result_id=0):
        self.mlir += '''
    %result_table{result_id} = subop.create_result_table ["x"] ->  !subop.result_table<[x_res{result_id} : f64]>
    %{stream1} = subop.scan_refs %{array_name} : !subop.array<[{xname} : f64]> @scan{result_id}::@ref({{type=!subop.continous_entry_ref<!subop.array<[{xname} : f64]>>}}) {{sequential}}
    %{stream2} = subop.gather %{stream1} @scan{result_id}::@ref {{ {xname} => @scan{result_id}::@currval({{type=f64}}) }}
    subop.materialize %{stream2} {{@scan{result_id}::@currval => x_res{result_id} }}, %result_table{result_id} : !subop.result_table<[x_res{result_id} : f64]>
    subop.set_result {result_id} %result_table{result_id}  : !subop.result_table<[x_res{result_id} : f64]>
        '''.format(**{"xname": array.xname, "array_name": array.varname, "stream1": self.get_stream_name(),
                      "stream2": self.get_stream_name(), "result_id": result_id})


class Array:
    def __init__(self, size, builder):
        self.size = size
        self.builder = builder
        self.xname = "x" + str(builder.x_ctr)
        builder.x_ctr += 1
        self.varname = builder.create_array(self.xname, self.size)

    def div_const(self, const):
        res = Array(self.size, self.builder)

        def fn(val):
            return '''%const = arith.constant {const} : f64
    %div = arith.divf %{val}, %const : f64
    tuples.return %div : f64
            '''.format(**{"val": val, "const": str(float(const))})

        self.builder.write_updated_to(self, res, fn)
        return res

    def square(self):
        res = Array(self.size, self.builder)

        def fn(val):
            return '''  %squared = arith.mulf %{val}, %{val} : f64
        tuples.return %squared : f64
                '''.format(**{"val": val})

        self.builder.write_updated_to(self, res, fn)
        return res

    def sub_const(self, const):
        res = Array(self.size, self.builder)

        def fn(val):
            return '''%const = arith.constant {const} : f64
        %sub = arith.subf %{val}, %const : f64
        tuples.return %sub : f64
                '''.format(**{"val": val, "const": str(float(const))})

        self.builder.write_updated_to(self, res, fn)
        return res

    def sub_const_rev(self, const):
        res = Array(self.size, self.builder)

        def fn(val):
            return '''%const = arith.constant {const} : f64
        %sub = arith.subf %const,%{val} : f64
        tuples.return %sub : f64
                '''.format(**{"val": val, "const": str(float(const))})

        self.builder.write_updated_to(self, res, fn)
        return res

    def add_const(self, const):
        res = Array(self.size, self.builder)

        def fn(val):
            return '''%const = arith.constant {const} : f64
        %sub = arith.addf %{val}, %const : f64
        tuples.return %sub : f64
                '''.format(**{"val": val, "const": str(float(const))})

        self.builder.write_updated_to(self, res, fn)
        return res

    def mul_const(self, const):
        res = Array(self.size, self.builder)

        def fn(val):
            return '''%const = arith.constant {const} : f64
        %mul = arith.mulf %{val}, %const : f64
        tuples.return %mul : f64
                '''.format(**{"val": val, "const": str(float(const))})

        self.builder.write_updated_to(self, res, fn)
        return res

    def mul(self, other):
        res = Array(self.size, self.builder)

        def fn(val, otherVal):
            return '''%mul = arith.mulf %{val}, %{otherVal} : f64
        tuples.return %mul : f64
                '''.format(**{"val": val, "otherVal": str(otherVal)})

        self.builder.write_updated_to(self, res, fn, other)
        return res

    def div(self, other):
        res = Array(self.size, self.builder)

        def fn(val, otherVal):
            return '''%div = arith.divf %{val}, %{otherVal} : f64
        tuples.return %div : f64
                '''.format(**{"val": val, "otherVal": str(otherVal)})

        self.builder.write_updated_to(self, res, fn, other)
        return res

    def sin(self):
        res = Array(self.size, self.builder)

        def fn(val):
            return '''
        %computed = db.runtime_call "Sin"  (%{val}) : (f64) -> f64
        tuples.return %computed : f64
                '''.format(**{"val": val})

        self.builder.write_updated_to(self, res, fn)
        return res

    def cos(self):
        res = Array(self.size, self.builder)

        def fn(val):
            return '''
        %computed = db.runtime_call "Cos"  (%{val}) : (f64) -> f64
        tuples.return %computed : f64
                '''.format(**{"val": val})

        self.builder.write_updated_to(self, res, fn)
        return res

    def sqrt(self):
        res = Array(self.size, self.builder)

        def fn(val):
            return '''
        %computed = db.runtime_call "Sqrt"  (%{val}) : (f64) -> f64
        tuples.return %computed : f64
                '''.format(**{"val": val})

        self.builder.write_updated_to(self, res, fn)
        return res

    def erf(self):
        res = Array(self.size, self.builder)

        def fn(val):
            return '''
        %computed = db.runtime_call "Erf"  (%{val}) : (f64) -> f64
        tuples.return %computed : f64
                '''.format(**{"val": val})

        self.builder.write_updated_to(self, res, fn)
        return res

    def exp(self):
        res = Array(self.size, self.builder)

        def fn(val):
            return '''
        %computed = db.runtime_call "Exp"  (%{val}) : (f64) -> f64
        tuples.return %computed : f64
                '''.format(**{"val": val})

        self.builder.write_updated_to(self, res, fn)
        return res

    def log(self):
        res = Array(self.size, self.builder)

        def fn(val):
            return '''
        %computed = db.runtime_call "Log"  (%{val}) : (f64) -> f64
        tuples.return %computed : f64
                '''.format(**{"val": val})

        self.builder.write_updated_to(self, res, fn)
        return res

    def arcsin(self):
        res = Array(self.size, self.builder)

        def fn(val):
            return '''
        %computed = db.runtime_call "ASin"  (%{val}) : (f64) -> f64
        tuples.return %computed : f64
                '''.format(**{"val": val})

        self.builder.write_updated_to(self, res, fn)
        return res

    def add(self, other):
        res = Array(self.size, self.builder)

        def fn(val, otherVal):
            return '''%mul = arith.addf %{val}, %{otherVal} : f64
        tuples.return %mul : f64
                '''.format(**{"val": val, "otherVal": str(otherVal)})

        self.builder.write_updated_to(self, res, fn, other)
        return res

    def sub(self, other):
        res = Array(self.size, self.builder)

        def fn(val, otherVal):
            return '''%mul = arith.subf %{val}, %{otherVal} : f64
        tuples.return %mul : f64
                '''.format(**{"val": val, "otherVal": str(otherVal)})

        self.builder.write_updated_to(self, res, fn, other)
        return res
